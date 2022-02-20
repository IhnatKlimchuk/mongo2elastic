using Notidar.Mongo2Elastic.Elasticsearch;
using Notidar.Mongo2Elastic.MongoDB;

namespace Notidar.Mongo2Elastic
{
    public class Replicator<TSourceDocument, TDestinationDocument> : IReplicator
        where TSourceDocument : class
        where TDestinationDocument : class
    {
        private readonly IStateRepository _replicationStateRepository;
        private readonly IDestinationRepository<TDestinationDocument> _destinationRepository;
        private readonly ISourceRepository<TSourceDocument> _sourceRepository;
        private readonly Func<TSourceDocument, TDestinationDocument> _map;
        private readonly ReplicatorOptions _options;

        private readonly Guid _replicatorId;

        public Replicator(
            IStateRepository replicationStateRepository,
            IDestinationRepository<TDestinationDocument> destinationRepository,
            ISourceRepository<TSourceDocument> sourceRepository,
            Func<TSourceDocument, TDestinationDocument> map,
            ReplicatorOptions options)
        {
            _replicationStateRepository = replicationStateRepository;
            _destinationRepository = destinationRepository;
            _sourceRepository = sourceRepository;
            _map = map;
            _options = options;

            _replicatorId = Guid.NewGuid();
        }

        public async Task ExecuteAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var state = await _replicationStateRepository.TryLockStateAsync(
                            replicatorId: _replicatorId,
                            lockExpirationDateUtc: DateTime.UtcNow.Add(_options.LockTimeout),
                            cancellationToken: cancellationToken);

                        if (state != null)
                        {
                            using var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                            Task? lockUpdateTask = null;
                            try
                            {
                                lockUpdateTask = Task.Run(
                                    function: () => RefreshStateAsync(
                                        state: state,
                                        cancellationTokenSource: cancellationTokenSource,
                                        cancellationToken: cancellationTokenSource.Token),
                                    cancellationToken: cancellationTokenSource.Token);

                                await ReplicateAsync(state, cancellationTokenSource.Token);
                            }
                            catch (Exception) when (cancellationTokenSource.Token.IsCancellationRequested)
                            {
                                // nothing
                            }
                            finally
                            {
                                cancellationTokenSource.Cancel();
                                if (lockUpdateTask != null)
                                {
                                    await lockUpdateTask;
                                }
                            }
                        }
                        else
                        {
                            await Task.Delay(_options.LockTimeout, cancellationToken);
                        }
                    }
                    catch (Exception)
                    {
                        //nothing 
                    }
                }
            }
            finally
            {
                await _replicationStateRepository.TryUnlockStateAsync(
                    replicatorId: _replicatorId,
                    cancellationToken: default);
            }
        }

        private async Task RefreshStateAsync(ReplicationState state, CancellationTokenSource cancellationTokenSource, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var resultState = await _replicationStateRepository.TryUpdateStateAsync(
                        replicatorId: _replicatorId,
                        version: state.Version,
                        resumeToken: state.ResumeToken,
                        lockExpirationDateUtc: DateTime.UtcNow.Add(_options.LockTimeout),
                        cancellationToken: cancellationToken);

                    if (resultState == null)
                    {
                        throw new InvalidOperationException("Failed to update replication state.");
                    }

                    await Task.Delay(_options.StateUpdateDelay, cancellationToken);
                }
                catch
                {
                    cancellationTokenSource.Cancel();
                }
            }
        }

        private async Task ReplicateAsync(ReplicationState state, CancellationToken cancellationToken)
        {
            using var stream = await GetStreamAsync(state, cancellationToken);

            if (state.ResumeToken == null)
            {
                state.Version++;
                await _destinationRepository.PrepareForSynchronizationAsync(state.Version, cancellationToken);
                await FullSyncAsync(state, cancellationToken);
            }

            await _destinationRepository.PrepareForReplicationAsync(state.Version, cancellationToken);
            await foreach (var batch in stream.WithCancellation(cancellationToken))
            {
                var resumeToken = stream.GetResumeToken();
                await SyncBatchAsync(state, batch, cancellationToken);
                state.ResumeToken = resumeToken;
            }
        }

        private async Task SyncBatchAsync(ReplicationState state, IEnumerable<Operation<TSourceDocument>> changes, CancellationToken cancellationToken)
        {
            if (!changes.Any())
            {
                return;
            }

            var resultBatch = new Dictionary<object, (OperationType Operation, TDestinationDocument Document)>();
            foreach (var change in changes)
            {
                resultBatch[change.Key] = (change.Type, _map(change.Document));
            }

            var operationToDocuments = resultBatch.ToLookup(x => x.Value.Operation, x => x.Value.Document);
            await _destinationRepository.BulkUpdateAsync(
                addOrUpdate: operationToDocuments[OperationType.AddOrUpdate],
                delete: operationToDocuments[OperationType.Delete],
                state.Version,
                cancellationToken: cancellationToken);
        }

        private async Task<IAsyncReplicationStream<TSourceDocument>> GetStreamAsync(ReplicationState state, CancellationToken cancellationToken)
        {
            return await _sourceRepository.TryGetStreamAsync(state.ResumeToken, cancellationToken)
                ?? await _sourceRepository.TryGetStreamAsync(resumeToken: state.ResumeToken = null, cancellationToken)
                ?? throw new InvalidOperationException();
        }

        private async Task FullSyncAsync(ReplicationState state, CancellationToken cancellationToken)
        {
            var batchEnumerator = await _sourceRepository.GetDocumentsAsync(cancellationToken: cancellationToken);
            await foreach (var batch in batchEnumerator)
            {
                if (batch.Any())
                {
                    await _destinationRepository.BulkUpdateAsync(batch.Select(_map), Enumerable.Empty<TDestinationDocument>(), state.Version, cancellationToken);
                }
            }
        }
    }
}
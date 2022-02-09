using MongoDB.Driver;
using Notidar.Mongo2Elastic.Destinations;
using Notidar.Mongo2Elastic.Sources;
using Notidar.Mongo2Elastic.States;

namespace Notidar.Mongo2Elastic
{
    public class GenericReplicator<TSourceDocument, TKey, TDestinationDocument> : IReplicator
        where TSourceDocument : class
        where TDestinationDocument : class
    {
        private readonly IReplicationStateRepository _replicationStateRepository;
        private readonly IDestinationRepository<TDestinationDocument> _destinationRepository;
        private readonly ISourceRepository<TSourceDocument> _sourceRepository;
        private readonly Func<TSourceDocument, TDestinationDocument>? _map;
        private readonly ReplicatorOptions _options;

        private readonly Guid _replicatorId;

        public GenericReplicator(
            IReplicationStateRepository replicationStateRepository,
            IDestinationRepository<TDestinationDocument> destinationRepository,
            ISourceRepository<TSourceDocument> sourceRepository,
            Func<TSourceDocument,TDestinationDocument> map,
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
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var state = await _replicationStateRepository.TryLockStateAsync(
                        replicationName: _options.ReplicationName,
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
                    // nothing
                }
            }
        }

        private async Task RefreshStateAsync(ReplicationState state, CancellationTokenSource cancellationTokenSource, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var resultState = await _replicationStateRepository.TryUpdateStateAsync(
                        replicationName: _options.ReplicationName,
                        replicatorId: _replicatorId,
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
                await FullSyncAsync(cancellationToken);
            }

            await foreach (var batch in stream.ToAsyncEnumerable(cancellationToken))
            {
                var resumeToken = stream.GetResumeToken();
                if (batch.Any())
                {
                    await SyncBatchAsync(batch, cancellationToken);
                }
                state.ResumeToken = resumeToken;
            }
        }

        private Task SyncBatchAsync(IEnumerable<ChangeStreamDocument<TSourceDocument>> changes, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        private async Task<IChangeStreamCursor<ChangeStreamDocument<TSourceDocument>>> GetStreamAsync(ReplicationState state, CancellationToken cancellationToken)
        {
            var stream = state.ResumeToken == null ? null : await _sourceRepository.TryRestoreStreamAsync(_options.MaxSourceAwaitTime, _options.BatchSize, state.ResumeToken, cancellationToken);

            if (stream == null)
            {
                state.ResumeToken = null;
            }

            return stream ?? await _sourceRepository.GetStreamAsync(_options.MaxSourceAwaitTime, _options.BatchSize, cancellationToken);
        }

        private async Task FullSyncAsync(CancellationToken cancellationToken)
        {
            //var amountToTransfer = await _sourceRepository.CountAsync(cancellationToken: cancellationToken);
            var batchEnumerator = await _sourceRepository.GetAllAsync(_options.BatchSize, cancellationToken: cancellationToken);
            await foreach (var batch in batchEnumerator)
            {
                if (batch.Any())
                {
                    await _destinationRepository.BulkAsync(batch.Select(_map), Enumerable.Empty<TDestinationDocument>(), cancellationToken);
                }
            }
        }
    }
}
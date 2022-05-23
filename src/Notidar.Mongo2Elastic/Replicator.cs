using Microsoft.Extensions.Logging;
using Notidar.Mongo2Elastic.Elasticsearch;
using Notidar.Mongo2Elastic.MongoDB;
using Notidar.Mongo2Elastic.State;

namespace Notidar.Mongo2Elastic
{
    public class Replicator<TSourceDocument, TDestinationDocument> : IReplicator
        where TSourceDocument : class
        where TDestinationDocument : class
    {
        private bool _isStopRequested = false;
        private Task _task = null;
        private CancellationTokenSource _cancellationTokenSource = new();

        private readonly IStateRepository _stateStore;
        private readonly IDestinationRepository<TDestinationDocument> _destinationRepository;
        private readonly ISourceRepository<TSourceDocument> _sourceRepository;
        private readonly Func<TSourceDocument, TDestinationDocument> _map;
        private readonly ILogger<Replicator<TSourceDocument, TDestinationDocument>>? _logger;

        public Replicator(
            IStateRepository stateStore,
            IDestinationRepository<TDestinationDocument> destinationRepository,
            ISourceRepository<TSourceDocument> sourceRepository,
            Func<TSourceDocument, TDestinationDocument> map,
            ILogger<Replicator<TSourceDocument, TDestinationDocument>> logger = null)
        {
            _stateStore = stateStore;
            _destinationRepository = destinationRepository;
            _sourceRepository = sourceRepository;
            _map = map;
            _logger = logger;
        }

        public async Task ExecuteAsync(CancellationToken cancellationToken = default)
        {
            while (!_isStopRequested)
            {
                try
                {
                    await using var stateHandler = await _stateStore.TryLockStateOrDefaultAsync(cancellationToken);

                    // get existing stream
                    using var restoredStream = stateHandler.ResumeToken != null ? await _sourceRepository.TryGetStreamAsync(stateHandler.ResumeToken, cancellationToken) : null;
                    if (restoredStream != null)
                    {
                        await ReplicateAsync(stateHandler, restoredStream, cancellationToken);
                    }
                    else
                    {
                        await SyncAndReplicateAsync(stateHandler, cancellationToken);
                    }
                }
                catch
                {
                    // do nothing
                }
            }
        }

        public Task StartAsync(CancellationToken _ = default)
        {
            if (_task != null)
            {
                throw new InvalidOperationException("Replication is already running.");
            }

            _task = ExecuteAsync(_cancellationTokenSource.Token);

            return Task.CompletedTask;
        }

        public async Task StopAsync(CancellationToken cancellationToken = default)
        {
            _isStopRequested = true;
            using var registration = cancellationToken.Register(() => _cancellationTokenSource.Cancel());
            await _task;
        }

        private async Task ReplicateAsync(IState stateHandler, IAsyncReplicationStream<TSourceDocument> stream, CancellationToken cancellationToken = default)
        {
            await _destinationRepository.PrepareForReplicationAsync(stateHandler.Version, cancellationToken);
            await foreach (var batch in stream.WithCancellation(cancellationToken))
            {
                if (batch.Any())
                {
                    var resultBatch = new Dictionary<object, (OperationType Operation, TDestinationDocument Document)>();
                    foreach (var change in batch)
                    {
                        resultBatch[change.Key] = (change.Type, _map(change.Document));
                    }

                    var operationToDocuments = resultBatch.ToLookup(x => x.Value.Operation, x => x.Value.Document);
                    await _destinationRepository.BulkUpdateAsync(
                        addOrUpdate: operationToDocuments[OperationType.AddOrUpdate],
                        delete: operationToDocuments[OperationType.Delete],
                        stateHandler.Version,
                        cancellationToken: cancellationToken);
                }
                await stateHandler.UpdateResumeTokenAsync(stream.GetResumeToken(), cancellationToken);

                if (_isStopRequested)
                {
                    break;
                }
            }
        }

        private async Task SyncAndReplicateAsync(IState stateHandler, CancellationToken cancellationToken = default)
        {
            using var freshStream = await _sourceRepository.TryGetStreamAsync(null, cancellationToken) ?? throw new InvalidOperationException();

            await stateHandler.UpdateResumeTokenAsync(null, cancellationToken);
            await stateHandler.UpdateVersionAsync(stateHandler.Version + 1, cancellationToken);

            await _destinationRepository.PrepareForSynchronizationAsync(stateHandler.Version, cancellationToken);
            var batchEnumerator = await _sourceRepository.GetDocumentsAsync(cancellationToken: cancellationToken);
            await foreach (var batch in batchEnumerator.WithCancellation(cancellationToken))
            {
                if (batch.Any())
                {
                    await _destinationRepository.BulkUpdateAsync(batch.Select(_map), Enumerable.Empty<TDestinationDocument>(), stateHandler.Version, cancellationToken);
                }

                if (_isStopRequested)
                {
                    break;
                }
            }

            await stateHandler.UpdateResumeTokenAsync(freshStream.GetResumeToken(), cancellationToken);

            await ReplicateAsync(stateHandler, freshStream, cancellationToken);
        }

        public async ValueTask DisposeAsync()
        {
            await DisposeAsyncCore().ConfigureAwait(false);
            GC.SuppressFinalize(this);
        }

        protected virtual async ValueTask DisposeAsyncCore()
        {
            if (_cancellationTokenSource is not null)
            {
                _cancellationTokenSource.Cancel();
            }
            if (_task is not null)
            {
                await _task.ConfigureAwait(false);
            }

            if (_cancellationTokenSource is not null)
            {
                _cancellationTokenSource.Dispose();
            }
            if (_task is not null)
            {
                _task.Dispose();
            }

            _cancellationTokenSource = null;
            _task = null;
        }
    }
}
using Notidar.Mongo2Elastic.Elasticsearch;
using Notidar.Mongo2Elastic.MongoDB;
using Notidar.Mongo2Elastic.State;

namespace Notidar.Mongo2Elastic
{
    public class Replicator<TSourceDocument, TDestinationDocument> : IReplicator
        where TSourceDocument : class
        where TDestinationDocument : class
    {
        private readonly IStateRepository _stateStore;
        private readonly IDestinationRepository<TDestinationDocument> _destinationRepository;
        private readonly ISourceRepository<TSourceDocument> _sourceRepository;
        private readonly Func<TSourceDocument, TDestinationDocument> _map;

        public Replicator(
            IStateRepository stateStore,
            IDestinationRepository<TDestinationDocument> destinationRepository,
            ISourceRepository<TSourceDocument> sourceRepository,
            Func<TSourceDocument, TDestinationDocument> map)
        {
            _stateStore = stateStore;
            _destinationRepository = destinationRepository;
            _sourceRepository = sourceRepository; 
            _map = map;
        }

        public async Task ExecuteAsync(CancellationToken cancellationToken = default)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await IntenalExecuteAsync(cancellationToken);
            }
        }

        public async Task IntenalExecuteAsync(CancellationToken cancellationToken = default)
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
            }

            await stateHandler.UpdateResumeTokenAsync(freshStream.GetResumeToken(), cancellationToken);

            await ReplicateAsync(stateHandler, freshStream, cancellationToken);
        }
    }
}
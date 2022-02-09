using MongoDB.Bson;
using MongoDB.Driver;

namespace Notidar.Mongo2Elastic.Sources
{
    public class SourceRepository<TSourceDocument, TKey> : ISourceRepository<TSourceDocument, TKey> where TSourceDocument : class
    {
        private readonly IMongoCollection<TSourceDocument> _documentCollection;
        private readonly Func<TSourceDocument, TKey> _getKey;
        public SourceRepository(
            IMongoCollection<TSourceDocument> documentCollection,
            Func<TSourceDocument, TKey> getKey)
        {
            _documentCollection = documentCollection;
            _getKey = getKey;
        }

        public async Task<IAsyncEnumerable<IEnumerable<TSourceDocument>>> GetAllAsync(int batchSize, CancellationToken cancellationToken = default)
        {
            var cursor = await _documentCollection.FindAsync(
                filter: Builders<TSourceDocument>.Filter.Empty,
                options: new FindOptions<TSourceDocument>
                {
                    BatchSize = batchSize,
                },
                cancellationToken: cancellationToken);

            return cursor.ToAsyncEnumerable(cancellationToken);
        }

        public async Task<IAsyncReplicationStream<TSourceDocument, TKey>> GetStreamAsync(TimeSpan maxAwaitTime, int batchSize, CancellationToken cancellationToken = default)
        {
            var streamCursor = await _documentCollection.WatchAsync(
                options: new ChangeStreamOptions
                {
                    MaxAwaitTime = maxAwaitTime,
                    FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                    StartAfter = null,
                    BatchSize = batchSize
                },
                cancellationToken: cancellationToken);

            return new MongoAsyncReplicationStream<TSourceDocument, TKey>(streamCursor, _getKey);
        }

        public async Task<IAsyncReplicationStream<TSourceDocument, TKey>> TryRestoreStreamAsync(TimeSpan maxAwaitTime, int batchSize, string resumeToken, CancellationToken cancellationToken = default)
        {
            try
            {
                var streamCursor = await _documentCollection.WatchAsync(
                    options: new ChangeStreamOptions
                    {
                        MaxAwaitTime = maxAwaitTime,
                        FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                        StartAfter = new BsonDocument("_data", resumeToken),
                        BatchSize = batchSize
                    },
                    cancellationToken: cancellationToken);
                return new MongoAsyncReplicationStream<TSourceDocument, TKey>(streamCursor, _getKey);
            }
            catch
            {
                return null;
            }
        }
    }
}

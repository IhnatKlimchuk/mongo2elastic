using MongoDB.Bson;
using MongoDB.Driver;

namespace Notidar.Mongo2Elastic.MongoDB
{
    public class SourceRepository<TSourceDocument, TKey> : ISourceRepository<TSourceDocument, TKey> where TSourceDocument : class
    {
        private readonly IMongoCollection<TSourceDocument> _documentCollection;
        private readonly Func<TSourceDocument, TKey> _getKey;
        private readonly TimeSpan _maxAwaitTime;
        public SourceRepository(
            IMongoCollection<TSourceDocument> documentCollection,
            TimeSpan maxAwaitTime,
            Func<TSourceDocument, TKey> getKey)
        {
            _documentCollection = documentCollection;
            _maxAwaitTime = maxAwaitTime;
            _getKey = getKey;
        }

        public async Task<IAsyncEnumerable<IEnumerable<TSourceDocument>>> GetDocumentsAsync(int batchSize, CancellationToken cancellationToken = default)
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

        public async Task<IAsyncReplicationStream<TSourceDocument, TKey>?> TryGetStreamAsync(int batchSize, string? resumeToken, CancellationToken cancellationToken = default)
        {
            try
            {
                var streamCursor = await _documentCollection.WatchAsync(
                    options: new ChangeStreamOptions
                    {
                        MaxAwaitTime = _maxAwaitTime,
                        FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                        StartAfter = resumeToken == null ? null : new BsonDocument("_data", resumeToken),
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

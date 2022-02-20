using MongoDB.Bson;
using MongoDB.Driver;

namespace Notidar.Mongo2Elastic.MongoDB
{
    public class SourceRepository<TDocument> : ISourceRepository<TDocument> where TDocument : class
    {
        private readonly IMongoCollection<TDocument> _documentCollection;
        private readonly TimeSpan _maxAwaitTime;
        public SourceRepository(
            IMongoCollection<TDocument> documentCollection,
            TimeSpan maxAwaitTime)
        {
            _documentCollection = documentCollection;
            _maxAwaitTime = maxAwaitTime;
        }

        public async Task<IAsyncEnumerable<IEnumerable<TDocument>>> GetDocumentsAsync(int batchSize, CancellationToken cancellationToken = default)
        {
            var cursor = await _documentCollection.FindAsync(
                filter: Builders<TDocument>.Filter.Empty,
                options: new FindOptions<TDocument>
                {
                    BatchSize = batchSize,
                },
                cancellationToken: cancellationToken);

            return cursor.ToAsyncEnumerable(cancellationToken);
        }

        public async Task<IAsyncReplicationStream<TDocument>?> TryGetStreamAsync(int batchSize, string? resumeToken, CancellationToken cancellationToken = default)
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
                return new MongoAsyncReplicationStream<TDocument>(streamCursor);
            }
            catch
            {
                return null;
            }
        }
    }
}

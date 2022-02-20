using MongoDB.Bson;
using MongoDB.Driver;

namespace Notidar.Mongo2Elastic.MongoDB
{
    public class SourceRepository<TSourceDocument> : ISourceRepository<TSourceDocument> where TSourceDocument : class
    {
        private readonly IMongoCollection<TSourceDocument> _documentCollection;
        private readonly TimeSpan _maxAwaitTime;
        public SourceRepository(
            IMongoCollection<TSourceDocument> documentCollection,
            TimeSpan maxAwaitTime)
        {
            _documentCollection = documentCollection;
            _maxAwaitTime = maxAwaitTime;
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

        public async Task<IAsyncReplicationStream<TSourceDocument>?> TryGetStreamAsync(int batchSize, string? resumeToken, CancellationToken cancellationToken = default)
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
                return new MongoAsyncReplicationStream<TSourceDocument>(streamCursor);
            }
            catch
            {
                return null;
            }
        }
    }
}

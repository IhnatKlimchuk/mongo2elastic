using MongoDB.Bson;
using MongoDB.Driver;

namespace Notidar.Mongo2Elastic.MongoDB
{
    public class SourceRepository<TDocument> : ISourceRepository<TDocument> where TDocument : class
    {
        private readonly IMongoCollection<TDocument> _documentCollection;
        private readonly SourceRepositoryOptions _options;
        public SourceRepository(
            IMongoCollection<TDocument> documentCollection,
            SourceRepositoryOptions options)
        {
            _documentCollection = documentCollection;
            _options = options;
        }

        public async Task<IAsyncEnumerable<IEnumerable<TDocument>>> GetDocumentsAsync(CancellationToken cancellationToken = default)
        {
            var cursor = await _documentCollection.FindAsync(
                filter: Builders<TDocument>.Filter.Empty,
                options: new FindOptions<TDocument>
                {
                    BatchSize = _options.BatchSize,
                },
                cancellationToken: cancellationToken);

            return cursor.ToAsyncEnumerable(cancellationToken);
        }

        public async Task<IAsyncReplicationStream<TDocument>?> TryGetStreamAsync(string? resumeToken, CancellationToken cancellationToken = default)
        {
            try
            {
                var streamCursor = await _documentCollection.WatchAsync(
                    options: new ChangeStreamOptions
                    {
                        MaxAwaitTime = _options.MaxAwaitTime,
                        FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                        StartAfter = resumeToken == null ? null : new BsonDocument("_data", resumeToken),
                        BatchSize = _options.BatchSize
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

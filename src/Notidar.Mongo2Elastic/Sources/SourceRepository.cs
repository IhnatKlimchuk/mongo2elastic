using MongoDB.Bson;
using MongoDB.Driver;

namespace Notidar.Mongo2Elastic.Sources
{
    public class SourceRepository<TSourceDocument> : ISourceRepository<TSourceDocument> where TSourceDocument : class
    {
        private readonly IMongoCollection<TSourceDocument> _documentCollection;
        public SourceRepository(IMongoCollection<TSourceDocument> documentCollection)
        {
            _documentCollection = documentCollection;
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

        public async Task<IChangeStreamCursor<ChangeStreamDocument<TSourceDocument>>> GetStreamAsync(TimeSpan maxAwaitTime, int batchSize, CancellationToken cancellationToken = default)
        {
            return await _documentCollection.WatchAsync(
                options: new ChangeStreamOptions
                {
                    MaxAwaitTime = maxAwaitTime,
                    FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                    StartAfter = null,
                    BatchSize = batchSize
                },
                cancellationToken: cancellationToken);
        }

        public async Task<IChangeStreamCursor<ChangeStreamDocument<TSourceDocument>>?> TryRestoreStreamAsync(TimeSpan maxAwaitTime, int batchSize, BsonDocument resumeToken, CancellationToken cancellationToken = default)
        {
            try
            {
                return await _documentCollection.WatchAsync(
                    options: new ChangeStreamOptions
                    {
                        MaxAwaitTime = maxAwaitTime,
                        FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                        StartAfter = resumeToken,
                        BatchSize = batchSize
                    },
                    cancellationToken: cancellationToken);
            }
            catch
            {
                return null;
            }
        }
    }
}

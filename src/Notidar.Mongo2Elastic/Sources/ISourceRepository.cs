using MongoDB.Bson;
using MongoDB.Driver;

namespace Notidar.Mongo2Elastic.Sources
{
    public interface ISourceRepository<TSourceDocument> where TSourceDocument : class
    {
        //Task<long> CountAsync(CancellationToken cancellationToken = default);
        Task<IAsyncEnumerable<IEnumerable<TSourceDocument>>> GetAllAsync(int batchSize, CancellationToken cancellationToken = default);
        Task<IChangeStreamCursor<ChangeStreamDocument<TSourceDocument>>> GetStreamAsync(TimeSpan maxAwaitTime, int batchSize, CancellationToken cancellationToken = default);
        Task<IChangeStreamCursor<ChangeStreamDocument<TSourceDocument>>?> TryRestoreStreamAsync(TimeSpan maxAwaitTime, int batchSize, BsonDocument resumeToken, CancellationToken cancellationToken = default);
    }
}

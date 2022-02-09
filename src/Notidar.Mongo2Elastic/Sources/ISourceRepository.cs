namespace Notidar.Mongo2Elastic.Sources
{
    public interface ISourceRepository<TSourceDocument, TKey> where TSourceDocument : class
    {
        Task<IAsyncEnumerable<IEnumerable<TSourceDocument>>> GetAllAsync(int batchSize, CancellationToken cancellationToken = default);
        Task<IAsyncReplicationStream<TSourceDocument, TKey>> GetStreamAsync(TimeSpan maxAwaitTime, int batchSize, CancellationToken cancellationToken = default);
        Task<IAsyncReplicationStream<TSourceDocument, TKey>> TryRestoreStreamAsync(TimeSpan maxAwaitTime, int batchSize, string resumeToken, CancellationToken cancellationToken = default);
    }
}

namespace Notidar.Mongo2Elastic.MongoDB
{
    public interface ISourceRepository<TDocument, TKey> where TDocument : class
    {
        Task<IAsyncEnumerable<IEnumerable<TDocument>>> GetDocumentsAsync(int batchSize, CancellationToken cancellationToken = default);
        Task<IAsyncReplicationStream<TDocument, TKey>?> TryGetStreamAsync(int batchSize, string? resumeToken, CancellationToken cancellationToken = default);
    }
}

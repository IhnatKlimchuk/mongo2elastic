namespace Notidar.Mongo2Elastic.MongoDB
{
    public interface ISourceRepository<TDocument> where TDocument : class
    {
        Task<IAsyncEnumerable<IEnumerable<TDocument>>> GetDocumentsAsync(CancellationToken cancellationToken = default);
        Task<IAsyncReplicationStream<TDocument>?> TryGetStreamAsync(string? resumeToken = null, CancellationToken cancellationToken = default);
    }
}

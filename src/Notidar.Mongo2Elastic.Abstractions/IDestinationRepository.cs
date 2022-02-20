namespace Notidar.Mongo2Elastic
{
    public interface IDestinationRepository<TDocument> where TDocument : class
    {
        Task BulkUpdateAsync(IEnumerable<TDocument> addOrUpdate, IEnumerable<TDocument> delete, string token, CancellationToken cancellationToken = default);
        Task PrepareForReplicationAsync(string token, CancellationToken cancellationToken = default);
        Task PrepareForSynchronizationAsync(string token, CancellationToken cancellationToken = default);
    }
}

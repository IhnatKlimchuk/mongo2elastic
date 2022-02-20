namespace Notidar.Mongo2Elastic.Elasticsearch
{
    public interface IDestinationRepository<TDocument> where TDocument : class
    {
        Task BulkUpdateAsync(IEnumerable<TDocument> addOrUpdate, IEnumerable<TDocument> delete, int version, CancellationToken cancellationToken = default);
        Task PrepareForReplicationAsync(int version, CancellationToken cancellationToken = default);
        Task PrepareForSynchronizationAsync(int version, CancellationToken cancellationToken = default);
    }
}

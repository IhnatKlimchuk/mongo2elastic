namespace Notidar.Mongo2Elastic.Destinations
{
    public interface IDestinationRepository<TDestinationDocument> where TDestinationDocument : class
    {
        Task BulkAsync(IEnumerable<TDestinationDocument> addOrUpdate, IEnumerable<TDestinationDocument> delete, CancellationToken cancellationToken = default);
        Task DeleteAllDocumentsAsync(CancellationToken cancellationToken = default);
        Task CreateIndexAsync(CancellationToken cancellationToken = default);
        Task DeleteIndexAsync(CancellationToken cancellationToken = default);
    }
}
    
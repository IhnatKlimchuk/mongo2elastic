using Elasticsearch.Net;
using Nest;

namespace Notidar.Mongo2Elastic.Elasticsearch
{
    public class DestinationRepository<TDocument> : IDestinationRepository<TDocument> where TDocument : class
    {
        protected readonly IElasticClient Client;
        protected readonly Refresh Refresh;
        public DestinationRepository(IElasticClient client, bool waitForRefresh = true)
        {
            Client = client ?? throw new ArgumentNullException(nameof(client));
            Refresh = waitForRefresh ? Refresh.WaitFor : Refresh.False;
        }

        public virtual Task BulkUpdateAsync(IEnumerable<TDocument> addOrUpdate, IEnumerable<TDocument> delete, int version, CancellationToken cancellationToken = default)
        {
            return BulkUpdateAsync(addOrUpdate, delete, version, Refresh, cancellationToken);
        }

        protected async Task BulkUpdateAsync(IEnumerable<TDocument> addOrUpdate, IEnumerable<TDocument> delete, int version, Refresh refresh, CancellationToken cancellationToken = default)
        {
            var response = await Client.BulkAsync(
                selector: b => b
                    .Index<TDocument>()
                    .Refresh(refresh)
                    .IndexMany(addOrUpdate)
                    .DeleteMany(delete),
                ct: cancellationToken);
            if (response.Errors)
            {
                throw new InvalidOperationException("Failed to update document in elastic.");
            }
        }

        public virtual Task PrepareForReplicationAsync(int version, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        public virtual async Task PrepareForSynchronizationAsync(int version, CancellationToken cancellationToken = default)
        {
            var response = await Client.DeleteByQueryAsync<TDocument>(del => del
                .Conflicts(Conflicts.Proceed)
                .Query(q => q.QueryString(qs => qs.Query("*"))), cancellationToken);
            if (!response.IsValid)
            {
                throw new InvalidOperationException("Failed to delete all documents from index.");
            }
        }
    }
}

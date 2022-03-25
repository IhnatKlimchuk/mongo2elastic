using Elasticsearch.Net;
using Nest;

namespace Notidar.Mongo2Elastic.Elasticsearch
{
    public class DestinationRepository<TDocument> : IDestinationRepository<TDocument> where TDocument : class
    {
        protected readonly IElasticClient Client;
        protected readonly DestinationRepositoryOptions Options;
        public DestinationRepository(IElasticClient client, DestinationRepositoryOptions options)
        {
            Client = client ?? throw new ArgumentNullException(nameof(client));
            Options = options;
        }

        public virtual Task BulkUpdateAsync(IEnumerable<TDocument> addOrUpdate, IEnumerable<TDocument> delete, int version, CancellationToken cancellationToken = default)
        {
            return BulkUpdateAsync(addOrUpdate, delete, Options.Refresh, cancellationToken);
        }

        protected async Task BulkUpdateAsync(IEnumerable<TDocument> addOrUpdate, IEnumerable<TDocument> delete, Refresh refresh, CancellationToken cancellationToken = default)
        {
            var response = await Client.BulkAsync(
                selector: b => b
                    .Index<TDocument>()
                    .Refresh(refresh switch
                    {
                        Refresh.True => global::Elasticsearch.Net.Refresh.True,
                        Refresh.False => global::Elasticsearch.Net.Refresh.False,
                        Refresh.WaitFor => global::Elasticsearch.Net.Refresh.WaitFor,
                        _ => global::Elasticsearch.Net.Refresh.True
                    })
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

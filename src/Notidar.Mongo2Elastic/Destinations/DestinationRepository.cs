using Elasticsearch.Net;
using Nest;

namespace Notidar.Mongo2Elastic.Destinations
{
    public class DestinationRepository<TDestinationDocument> : IDestinationRepository<TDestinationDocument> where TDestinationDocument : class
    {
        private readonly IElasticClient _client;
        private readonly string _indexName;
        public DestinationRepository(IElasticClient client, string indexName)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _indexName = string.IsNullOrWhiteSpace(indexName) ? throw new ArgumentNullException(nameof(indexName)) : indexName;
        }

        public Task BulkAsync(
            IEnumerable<TDestinationDocument> addOrUpdate,
            IEnumerable<TDestinationDocument> delete,
            CancellationToken cancellationToken = default)
        {
            return BulkAsync(addOrUpdate, delete, Refresh.False, cancellationToken);
        }

        public async Task BulkAsync(
            IEnumerable<TDestinationDocument> addOrUpdate,
            IEnumerable<TDestinationDocument> delete,
            Refresh refresh, CancellationToken
            cancellationToken = default)
        {
            var response = await _client.BulkAsync(
                selector: b => b
                    .Index(_indexName)
                    .Refresh(refresh)
                    .IndexMany(addOrUpdate)
                    .DeleteMany(delete),
                ct: cancellationToken);
            if (response.Errors)
            {
                throw new InvalidOperationException("Failed to update document in elastic.");
            }
        }

        public async Task<long> CountAsync(CancellationToken cancellationToken = default)
        {
            var result = await _client.CountAsync<TDestinationDocument>(x => x.Index(_indexName), cancellationToken);
            return result.Count;
        }

        public async Task CreateIndexAsync(CancellationToken cancellationToken = default)
        {
            var result = await _client.Indices.GetAsync(_indexName, ct: cancellationToken);
            if (result.Indices.ContainsKey(_indexName))
            {
                return;
            }
            return;
        }

        public Task DeleteAllDocumentsAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public async Task DeleteIndexAsync(CancellationToken cancellationToken = default)
        {
            var result = await _client.Indices.GetAsync(_indexName, ct: cancellationToken);
            if (result.Indices.ContainsKey(_indexName))
            {
                var response = await _client.Indices.DeleteAsync(_indexName, ct: cancellationToken);
                if (!response.IsValid)
                {
                    throw new InvalidOperationException("Failed to delete documents index.");
                }
            }
        }

        public async Task PurgeIndexAsync(CancellationToken cancellationToken = default)
        {
            var response = await _client.DeleteByQueryAsync<TDestinationDocument>(del => del
                .Conflicts(Conflicts.Proceed)
                .Query(q => q.QueryString(qs => qs.Query("*"))), cancellationToken);
            if (!response.IsValid)
            {
                throw new InvalidOperationException("Failed to delete all documents from index.");
            }
        }
    }
}

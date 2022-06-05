using Elasticsearch.Net;
using Nest;

namespace Notidar.Mongo2Elastic.Elasticsearch
{
    public class VersionedDestinationRepository<TDocument> : DestinationRepository<TDocument> where TDocument : class, IVersionedDocument
    {
        public VersionedDestinationRepository(IElasticClient client, DestinationRepositoryOptions options) : base(client, options) { }

        public override Task BulkUpdateAsync(IEnumerable<TDocument> addOrUpdate, IEnumerable<TDocument> delete, int version, CancellationToken cancellationToken = default)
        {
            var versionEnrichedDocuments = addOrUpdate.Select(x =>
            {
                x.Mongo2ElasticReplicationVersion = version;
                return x;
            });

            return BulkUpdateAsync(versionEnrichedDocuments, delete, Options.Refresh, cancellationToken);
        }

        public override async Task PrepareForReplicationAsync(int version, CancellationToken cancellationToken = default)
        {
            var response = await Client.DeleteByQueryAsync<TDocument>(del => del
                .Conflicts(Conflicts.Proceed)
                .Query(q => q.Bool(b => b.MustNot(m => m.Term(t => t.Mongo2ElasticReplicationVersion, version)))), cancellationToken);
            if (!response.IsValid)
            {
                throw new InvalidOperationException("Failed to delete outdated documents from index.");
            }
        }

        public override Task PrepareForSynchronizationAsync(int version, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }
    }
}

using Nest;
using Notidar.Mongo2Elastic.Builder;

namespace Notidar.Mongo2Elastic.Elasticsearch.Builder
{
    public static class ReplicatorBuilderExtensions
    {
        public static IReplicatorBuilder<TSource, TDestination> ToElasticsearchWithVersions<TSource, TDestination>(
            this IReplicatorBuilder<TSource, TDestination> replicatorBuilder, IElasticClient client, Action<DestinationRepositoryOptions> configureAction = null)
            where TSource : class
            where TDestination : class, IVersionedDocument
        {
            var options = new DestinationRepositoryOptions();
            configureAction?.Invoke(options);
            return replicatorBuilder.Add(new VersionedDestinationRepository<TDestination>(client, options));
        }

        public static IReplicatorBuilder<TSource, TDestination> ToElasticsearchWithReset<TSource, TDestination>(
            this IReplicatorBuilder<TSource, TDestination> replicatorBuilder, IElasticClient client, Action<DestinationRepositoryOptions> configureAction = null)
            where TSource : class
            where TDestination : class
        {
            var options = new DestinationRepositoryOptions();
            configureAction?.Invoke(options);
            return replicatorBuilder.Add(new DestinationRepository<TDestination>(client, options));
        }
    }
}

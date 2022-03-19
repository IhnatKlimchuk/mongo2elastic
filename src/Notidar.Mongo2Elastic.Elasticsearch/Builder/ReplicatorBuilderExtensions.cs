using Nest;
using Notidar.Mongo2Elastic.Builder;

namespace Notidar.Mongo2Elastic.Elasticsearch.Builder
{
    public static class ReplicatorBuilderExtensions
    {
        public static IReplicatorStateBuilder<TSource, TDestination> ToElasticsearchWithVersions<TSource, TDestination>(
            this IReplicatorDestinationBuilder<TSource, TDestination> replicatorBuilder, IElasticClient client, Action<DestinationRepositoryOptions> configureAction = null)
            where TSource : class
            where TDestination : class, IVersionedDocument
        {
            var options = new DestinationRepositoryOptions();
            configureAction?.Invoke(options);
            return replicatorBuilder.Add(new VersionedDestinationRepository<TDestination>(client, options));
        }

        public static IReplicatorStateBuilder<TSource, TDestination> ToElasticsearchWithReset<TSource, TDestination>(
            this IReplicatorDestinationBuilder<TSource, TDestination> replicatorBuilder, IElasticClient client, Action<DestinationRepositoryOptions> configureAction = null)
            where TSource : class
            where TDestination : class
        {
            var options = new DestinationRepositoryOptions();
            configureAction?.Invoke(options);
            return replicatorBuilder.Add(new DestinationRepository<TDestination>(client, options));
        }
    }
}

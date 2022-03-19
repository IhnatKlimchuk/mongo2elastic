using Notidar.Mongo2Elastic.Elasticsearch;

namespace Notidar.Mongo2Elastic.Builder
{
    public interface IReplicatorDestinationBuilder<TSource, TDestination>
        where TDestination : class
        where TSource : class
    {
        IReplicatorStateBuilder<TSource, TDestination> Add(IDestinationRepository<TDestination> destinationRepository);
    }
}

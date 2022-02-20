using Notidar.Mongo2Elastic.Elasticsearch;
using Notidar.Mongo2Elastic.MongoDB;

namespace Notidar.Mongo2Elastic.Builder
{
    public interface IReplicatorBuilder<TSource, TDestination>
        where TDestination : class
        where TSource : class
    {
        IReplicatorBuilder<TSource, TDestination> Add(ISourceRepository<TSource> sourceRepository);
        IReplicatorBuilder<TSource, TDestination> Add(IDestinationRepository<TDestination> destinationRepository);
        IReplicatorBuilder<TSource, TDestination> Add(IStateRepository stateRepository);
        IReplicator Build();
    }
}

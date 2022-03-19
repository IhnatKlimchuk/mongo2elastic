using Notidar.Mongo2Elastic.MongoDB;

namespace Notidar.Mongo2Elastic.Builder
{
    public interface IReplicatorSourceBuilder<TSource, TDestination>
        where TDestination : class
        where TSource : class
    {
        IReplicatorDestinationBuilder<TSource, TDestination> Add(ISourceRepository<TSource> sourceRepository);
    }
}

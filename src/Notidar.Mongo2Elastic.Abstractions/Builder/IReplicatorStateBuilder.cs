using Notidar.Mongo2Elastic.State;

namespace Notidar.Mongo2Elastic.Builder
{
    public interface IReplicatorStateBuilder<TSource, TDestination>
        where TDestination : class
        where TSource : class
    {
        IReplicatorBuilder<TSource, TDestination> Add(IStateRepository stateRepository);
    }
}

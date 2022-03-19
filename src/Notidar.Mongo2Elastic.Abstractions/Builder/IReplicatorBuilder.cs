namespace Notidar.Mongo2Elastic.Builder
{
    public interface IReplicatorBuilder<TSource, TDestination>
        where TDestination : class
        where TSource : class
    {
        IReplicator Build();
    }
}

namespace Notidar.Mongo2Elastic.Builder
{
    public static class ReplicationBuilder
    {
        public static IReplicatorSourceBuilder<TSource, TSource> For<TSource>()
            where TSource : class
            => new ReplicatorBuilder<TSource, TSource>(x => x);
        public static IReplicatorSourceBuilder<TSource, TDestination> For<TSource, TDestination>(
            Func<TSource, TDestination> map)
            where TSource : class
            where TDestination : class
            => new ReplicatorBuilder<TSource, TDestination>(map);
    }
}

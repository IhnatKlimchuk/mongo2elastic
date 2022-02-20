namespace Notidar.Mongo2Elastic.Builder
{
    public static class ReplicationBuilder
    {
        public static ReplicatorBuilder<TSource, TSource> For<TSource>(Action<ReplicatorOptions> configureAction = null)
            where TSource : class
            => new(x => x, configureAction);
        public static ReplicatorBuilder<TSource, TDestination> For<TSource, TDestination>(Func<TSource, TDestination> map, Action<ReplicatorOptions> configureAction = null)
            where TSource : class
            where TDestination : class
            => new(map, configureAction);
    }
}

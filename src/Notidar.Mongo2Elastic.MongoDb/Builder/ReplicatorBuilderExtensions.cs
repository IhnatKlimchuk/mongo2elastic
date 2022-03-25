using MongoDB.Driver;
using Notidar.Mongo2Elastic.Builder;

namespace Notidar.Mongo2Elastic.MongoDB.Builder
{
    public static class ReplicatorBuilderExtensions
    {
        public static IReplicatorDestinationBuilder<TSource, TDestination> FromMongoDb<TSource, TDestination>(
            this IReplicatorSourceBuilder<TSource, TDestination> replicatorBuilder,
            IMongoCollection<TSource> mongoCollection,
            Action<SourceRepositoryOptions<TSource>> configureAction = null)
            where TSource : class
            where TDestination : class
        {
            var options = new SourceRepositoryOptions<TSource>();
            configureAction?.Invoke(options);
            return replicatorBuilder.Add(new SourceRepository<TSource>(mongoCollection, options));
        }

        public static IReplicatorDestinationBuilder<TSource, TDestination> FromMongoDb<TSource, TDestination>(
            this IReplicatorSourceBuilder<TSource, TDestination> replicatorBuilder,
            string mongoConnectionString,
            string collectionName,
            string databaseName = null,
            Action<SourceRepositoryOptions<TSource>> configureAction = null)
            where TSource : class
            where TDestination : class
        {
            var url = MongoUrl.Create(mongoConnectionString);
            var client = new MongoClient(url);
            var db = client.GetDatabase(databaseName ?? url.DatabaseName ?? throw new ArgumentException("Unknown mongo database."));
            var collection = db.GetCollection<TSource>(collectionName);

            return replicatorBuilder.FromMongoDb(collection, configureAction);
        }

        public static IReplicatorBuilder<TSource, TDestination> WithMongoDbState<TSource, TDestination>(
            this IReplicatorStateBuilder<TSource, TDestination> replicatorBuilder,
            IMongoCollection<ReplicationState> mongoCollection,
            string replicationId)
            where TSource : class
            where TDestination : class
        {
            return replicatorBuilder.Add(new MongoReplicationStateRepository(mongoCollection, replicationId));
        }

        public static IReplicatorBuilder<TSource, TDestination> WithMongoDbState<TSource, TDestination>(
            this IReplicatorStateBuilder<TSource, TDestination> replicatorBuilder,
            string mongoConnectionString,
            string replicationId,
            string databaseName = null,
            string collectionName = null)
            where TSource : class
            where TDestination : class
        {
            var url = MongoUrl.Create(mongoConnectionString);
            var client = new MongoClient(url);
            var db = client.GetDatabase(databaseName ?? url.DatabaseName ?? throw new ArgumentException("Unknown mongo database."));
            var collection = db.GetCollection<ReplicationState>(collectionName ?? "mongo2elastic-replications");
            
            return replicatorBuilder.WithMongoDbState(collection, replicationId);
        }
    }
}

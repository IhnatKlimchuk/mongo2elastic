﻿using MongoDB.Driver;
using Notidar.Mongo2Elastic.Builder;

namespace Notidar.Mongo2Elastic.MongoDB.Builder
{
    public static class ReplicatorBuilderExtensions
    {
        public static IReplicatorBuilder<TSource, TDestination> FromMongoDb<TSource, TDestination>(
            this IReplicatorBuilder<TSource, TDestination> replicatorBuilder, IMongoCollection<TSource> mongoCollection, Action<SourceRepositoryOptions> configureAction = null)
            where TSource : class
            where TDestination : class
        {
            var options = new SourceRepositoryOptions();
            configureAction?.Invoke(options);
            return replicatorBuilder.Add(new SourceRepository<TSource>(mongoCollection, options));
        }

        public static IReplicatorBuilder<TSource, TDestination> WithMongoDbState<TSource, TDestination>(
            this IReplicatorBuilder<TSource, TDestination> replicatorBuilder, IMongoCollection<ReplicationState> mongoCollection, string replicationId)
            where TSource : class
            where TDestination : class
        {
            return replicatorBuilder.Add(new MongoReplicationStateRepository(mongoCollection, replicationId));
        }
    }
}
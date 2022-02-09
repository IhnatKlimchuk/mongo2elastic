using MongoDB.Bson;
using MongoDB.Driver;

namespace Notidar.Mongo2Elastic.States
{
    public class MongoReplicationStateRepository : IReplicationStateRepository
    {
        private readonly IMongoCollection<ReplicationState> _stateCollection;

        public MongoReplicationStateRepository(IMongoCollection<ReplicationState> stateCollection)
        {
            _stateCollection = stateCollection ?? throw new ArgumentNullException(nameof(stateCollection));
        }

        public Task<ReplicationState?> TryLockStateAsync(
            string replicationName,
            Guid replicatorId,
            DateTime lockExpirationDateUtc,
            CancellationToken cancellationToken = default)
        {
            var utcNow = DateTime.UtcNow;
            return _stateCollection
                .FindOneAndUpdateAsync(
                    filter: Builders<ReplicationState>.Filter.And(
                        Builders<ReplicationState>.Filter.Eq(document => document.ReplicationKey, replicationName),
                        Builders<ReplicationState>.Filter.Or(
                            Builders<ReplicationState>.Filter.Eq(document => document.LockExpirationDateUtc, BsonNull.Value.ToNullableUniversalTime()),
                            Builders<ReplicationState>.Filter.Lte(document => document.LockExpirationDateUtc, utcNow),
                            Builders<ReplicationState>.Filter.Eq(document => document.ReplicatorId, BsonNull.Value.AsNullableGuid),
                            Builders<ReplicationState>.Filter.Eq(document => document.ReplicatorId, replicatorId))),
                    update: Builders<ReplicationState>.Update.Combine(
                        Builders<ReplicationState>.Update.Set(document => document.ReplicatorId, replicatorId),
                        Builders<ReplicationState>.Update.Set(document => document.LockExpirationDateUtc, lockExpirationDateUtc),
                        Builders<ReplicationState>.Update.Set(document => document.UpdatedAtUtc, DateTime.UtcNow)),
                    options: new FindOneAndUpdateOptions<ReplicationState> { IsUpsert = false, ReturnDocument = ReturnDocument.After },
                    cancellationToken: cancellationToken);
        }

        public Task<ReplicationState?> TryUpdateStateAsync(
            string replicationName,
            Guid replicatorId,
            DateTime lockExpirationDateUtc,
            BsonDocument? resumeToken = null,
            CancellationToken cancellationToken = default)
        {
            return _stateCollection
                .FindOneAndUpdateAsync(
                    filter: Builders<ReplicationState>.Filter.And(
                        Builders<ReplicationState>.Filter.Eq(document => document.ReplicationKey, replicationName),
                        Builders<ReplicationState>.Filter.Eq(document => document.ReplicatorId, replicatorId)),
                    update: Builders<ReplicationState>.Update.Combine(
                        Builders<ReplicationState>.Update.Set(document => document.LockExpirationDateUtc, lockExpirationDateUtc),
                        Builders<ReplicationState>.Update.Set(document => document.ResumeToken, resumeToken),
                        Builders<ReplicationState>.Update.Set(document => document.UpdatedAtUtc, DateTime.UtcNow)),
                    options: new FindOneAndUpdateOptions<ReplicationState> { IsUpsert = false, ReturnDocument = ReturnDocument.After },
                    cancellationToken: cancellationToken);
        }
    }
}

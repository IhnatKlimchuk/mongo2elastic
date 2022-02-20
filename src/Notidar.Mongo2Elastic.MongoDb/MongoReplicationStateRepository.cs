using MongoDB.Bson;
using MongoDB.Driver;

namespace Notidar.Mongo2Elastic.MongoDB
{
    public class MongoReplicationStateRepository : IReplicationStateRepository
    {
        private readonly IMongoCollection<ReplicationState> _stateCollection;
        private readonly string _replicationId;

        public MongoReplicationStateRepository(IMongoCollection<ReplicationState> stateCollection, string replicationId)
        {
            _stateCollection = stateCollection ?? throw new ArgumentNullException(nameof(stateCollection));
            _replicationId = replicationId;
        }

        public Task<ReplicationState?> TryLockStateAsync(
            Guid replicatorId,
            DateTime lockExpirationDateUtc,
            CancellationToken cancellationToken = default)
        {
            var utcNow = DateTime.UtcNow;
            return _stateCollection
                .FindOneAndUpdateAsync(
                    filter: Builders<ReplicationState>.Filter.And(
                        Builders<ReplicationState>.Filter.Eq(document => document.Id, _replicationId),
                        Builders<ReplicationState>.Filter.Or(
                            Builders<ReplicationState>.Filter.Eq(document => document.LockExpirationDateUtc, BsonNull.Value.ToNullableUniversalTime()),
                            Builders<ReplicationState>.Filter.Lte(document => document.LockExpirationDateUtc, utcNow),
                            Builders<ReplicationState>.Filter.Eq(document => document.LockReplicatorId, BsonNull.Value.AsNullableGuid),
                            Builders<ReplicationState>.Filter.Eq(document => document.LockReplicatorId, replicatorId))),
                    update: Builders<ReplicationState>.Update.Combine(
                        Builders<ReplicationState>.Update.Set(document => document.LockReplicatorId, replicatorId),
                        Builders<ReplicationState>.Update.Set(document => document.LockExpirationDateUtc, lockExpirationDateUtc),
                        Builders<ReplicationState>.Update.Set(document => document.UpdatedAtUtc, DateTime.UtcNow)),
                    options: new FindOneAndUpdateOptions<ReplicationState> { IsUpsert = false, ReturnDocument = ReturnDocument.After },
                    cancellationToken: cancellationToken);
        }

        public Task TryUnlockStateAsync(
            Guid replicatorId,
            CancellationToken cancellationToken = default)
        {
            var utcNow = DateTime.UtcNow;
            return _stateCollection
                .FindOneAndUpdateAsync(
                    filter: Builders<ReplicationState>.Filter.And(
                        Builders<ReplicationState>.Filter.Eq(document => document.Id, _replicationId),
                        Builders<ReplicationState>.Filter.Eq(document => document.LockReplicatorId, replicatorId)),
                    update: Builders<ReplicationState>.Update.Combine(
                        Builders<ReplicationState>.Update.Set(document => document.LockReplicatorId, BsonNull.Value.AsNullableGuid),
                        Builders<ReplicationState>.Update.Set(document => document.LockExpirationDateUtc, BsonNull.Value.ToNullableUniversalTime()),
                        Builders<ReplicationState>.Update.Set(document => document.UpdatedAtUtc, DateTime.UtcNow)),
                    options: new FindOneAndUpdateOptions<ReplicationState> { IsUpsert = false, ReturnDocument = ReturnDocument.After },
                    cancellationToken: cancellationToken);
        }

        public Task<ReplicationState?> TryUpdateStateAsync(
            Guid replicatorId,
            DateTime lockExpirationDateUtc,
            int version,
            string? resumeToken = null,
            CancellationToken cancellationToken = default)
        {
            return _stateCollection
                .FindOneAndUpdateAsync(
                    filter: Builders<ReplicationState>.Filter.And(
                        Builders<ReplicationState>.Filter.Eq(document => document.Id, _replicationId),
                        Builders<ReplicationState>.Filter.Eq(document => document.LockReplicatorId, replicatorId)),
                    update: Builders<ReplicationState>.Update.Combine(
                        Builders<ReplicationState>.Update.Set(document => document.LockExpirationDateUtc, lockExpirationDateUtc),
                        Builders<ReplicationState>.Update.Set(document => document.Version, version),
                        Builders<ReplicationState>.Update.Set(document => document.ResumeToken, resumeToken),
                        Builders<ReplicationState>.Update.Set(document => document.UpdatedAtUtc, DateTime.UtcNow)),
                    options: new FindOneAndUpdateOptions<ReplicationState> { IsUpsert = false, ReturnDocument = ReturnDocument.After },
                    cancellationToken: cancellationToken);
        }
    }
}

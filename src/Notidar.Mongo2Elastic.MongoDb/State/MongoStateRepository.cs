using MongoDB.Bson;
using MongoDB.Driver;

namespace Notidar.Mongo2Elastic.State
{
    public class MongoStateRepository : IStateRepository
    {
        private readonly IMongoCollection<StateMongoDbDocument> _stateCollection;
        private readonly string _replicationId;
        private readonly TimeSpan _lockExpiration;

        public MongoStateRepository(IMongoCollection<StateMongoDbDocument> stateCollection, string replicationId)
        {
            _stateCollection = stateCollection ?? throw new ArgumentNullException(nameof(stateCollection));
            _replicationId = replicationId;
            _lockExpiration = TimeSpan.FromSeconds(60);
        }

        public async Task<IState?> TryLockStateOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            var replicatorId = Guid.NewGuid();
            var document = await TryLockMongoStateAsync(replicatorId, cancellationToken);
            if (document == null)
            {
                return null;
            }
            return new SyncStateHandler(_stateCollection, document, replicatorId, _replicationId, _lockExpiration);
        }

        public Task<StateMongoDbDocument?> TryLockMongoStateAsync(
            Guid replicatorId,
            CancellationToken cancellationToken = default)
        {
            var utcNow = DateTime.UtcNow;
            var lockExpirationDateUtc = utcNow.Add(_lockExpiration);
            return _stateCollection
                .FindOneAndUpdateAsync(
                    filter: Builders<StateMongoDbDocument>.Filter.And(
                        Builders<StateMongoDbDocument>.Filter.Eq(document => document.Id, _replicationId),
                        Builders<StateMongoDbDocument>.Filter.Or(
                            Builders<StateMongoDbDocument>.Filter.Eq(document => document.LockExpirationDateUtc, BsonNull.Value.ToNullableUniversalTime()),
                            Builders<StateMongoDbDocument>.Filter.Lte(document => document.LockExpirationDateUtc, utcNow),
                            Builders<StateMongoDbDocument>.Filter.Eq(document => document.LockReplicatorId, BsonNull.Value.AsNullableGuid),
                            Builders<StateMongoDbDocument>.Filter.Eq(document => document.LockReplicatorId, replicatorId))),
                    update: Builders<StateMongoDbDocument>.Update.Combine(
                        Builders<StateMongoDbDocument>.Update.Set(document => document.LockReplicatorId, replicatorId),
                        Builders<StateMongoDbDocument>.Update.Set(document => document.LockExpirationDateUtc, lockExpirationDateUtc),
                        Builders<StateMongoDbDocument>.Update.Set(document => document.UpdatedAtUtc, DateTime.UtcNow)),
                    options: new FindOneAndUpdateOptions<StateMongoDbDocument> { IsUpsert = false, ReturnDocument = ReturnDocument.After },
                    cancellationToken: cancellationToken);
        }
    }
}

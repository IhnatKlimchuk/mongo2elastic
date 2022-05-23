using MongoDB.Bson;
using MongoDB.Driver;

namespace Notidar.Mongo2Elastic.State
{
    public sealed class SyncStateHandler : IState
    {
        private readonly IMongoCollection<StateMongoDbDocument> _stateCollection;
        private readonly StateMongoDbDocument _document;
        private readonly string _replicationId;
        private readonly TimeSpan _lockExpiration;
        private readonly Guid _replicatorId;
        public SyncStateHandler(
            IMongoCollection<StateMongoDbDocument> stateCollection,
            StateMongoDbDocument document,
            Guid replicatorId,
            string replicationId,
            TimeSpan lockExpiration)
        {
            _stateCollection = stateCollection;
            _document = document;
            _replicationId = replicationId;
            _lockExpiration = lockExpiration;
            _replicatorId = replicatorId;
        }

        public string? ResumeToken => _document.ResumeToken;

        public int Version => _document.Version;

        public Task UpdateResumeTokenAsync(string? resumeToken, CancellationToken cancellationToken = default)
        {
            var utcNow = DateTime.UtcNow;
            var lockExpirationDateUtc = utcNow.Add(_lockExpiration);
            return _stateCollection
                .FindOneAndUpdateAsync(
                    filter: Builders<StateMongoDbDocument>.Filter.And(
                        Builders<StateMongoDbDocument>.Filter.Eq(document => document.Id, _replicationId),
                        Builders<StateMongoDbDocument>.Filter.Eq(document => document.LockReplicatorId, _replicatorId)),
                    update: Builders<StateMongoDbDocument>.Update.Combine(
                        Builders<StateMongoDbDocument>.Update.Set(document => document.LockExpirationDateUtc, lockExpirationDateUtc),
                        Builders<StateMongoDbDocument>.Update.Set(document => document.ResumeToken, resumeToken),
                        Builders<StateMongoDbDocument>.Update.Set(document => document.UpdatedAtUtc, utcNow)),
                    options: new FindOneAndUpdateOptions<StateMongoDbDocument> { IsUpsert = false, ReturnDocument = ReturnDocument.After },
                    cancellationToken: cancellationToken);
        }

        public Task UpdateVersionAsync(int version, CancellationToken cancellationToken = default)
        {
            var utcNow = DateTime.UtcNow;
            var lockExpirationDateUtc = utcNow.Add(_lockExpiration);
            return _stateCollection
                .FindOneAndUpdateAsync(
                    filter: Builders<StateMongoDbDocument>.Filter.And(
                        Builders<StateMongoDbDocument>.Filter.Eq(document => document.Id, _replicationId),
                        Builders<StateMongoDbDocument>.Filter.Eq(document => document.LockReplicatorId, _replicatorId)),
                    update: Builders<StateMongoDbDocument>.Update.Combine(
                        Builders<StateMongoDbDocument>.Update.Set(document => document.LockExpirationDateUtc, lockExpirationDateUtc),
                        Builders<StateMongoDbDocument>.Update.Set(document => document.Version, version),
                        Builders<StateMongoDbDocument>.Update.Set(document => document.UpdatedAtUtc, utcNow)),
                    options: new FindOneAndUpdateOptions<StateMongoDbDocument> { IsUpsert = false, ReturnDocument = ReturnDocument.After },
                    cancellationToken: cancellationToken);
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                var utcNow = DateTime.UtcNow;
                await _stateCollection
                    .FindOneAndUpdateAsync(
                        filter: Builders<StateMongoDbDocument>.Filter.And(
                            Builders<StateMongoDbDocument>.Filter.Eq(document => document.Id, _replicationId),
                            Builders<StateMongoDbDocument>.Filter.Eq(document => document.LockReplicatorId, _replicatorId)),
                        update: Builders<StateMongoDbDocument>.Update.Combine(
                            Builders<StateMongoDbDocument>.Update.Set(document => document.LockReplicatorId, BsonNull.Value.AsNullableGuid),
                            Builders<StateMongoDbDocument>.Update.Set(document => document.LockExpirationDateUtc, BsonNull.Value.ToNullableUniversalTime()),
                            Builders<StateMongoDbDocument>.Update.Set(document => document.UpdatedAtUtc, utcNow)),
                        options: new FindOneAndUpdateOptions<StateMongoDbDocument> { IsUpsert = false, ReturnDocument = ReturnDocument.After },
                        cancellationToken: CancellationToken.None);
            }
            catch
            {
            }
        }
    }
}

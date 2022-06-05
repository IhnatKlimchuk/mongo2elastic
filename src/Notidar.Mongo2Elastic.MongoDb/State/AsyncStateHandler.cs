using MongoDB.Bson;
using MongoDB.Driver;

namespace Notidar.Mongo2Elastic.State
{
    public sealed class AsyncStateHandler : IState
    {
        private readonly IMongoCollection<StateMongoDbDocument> _stateCollection;
        private readonly StateMongoDbDocument _document;
        private readonly string _replicationId;
        private readonly TimeSpan _lockExpiration;
        private readonly Guid _replicatorId;

        private readonly Task _syncTask;
        private Task _requestTask;
        private 
        public AsyncStateHandler(
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
            _syncTask = Task.Run(async () => {
                while (true)
                {
                    _requestTask = SyncStateAsync();
                    await _requestTask;
                }
            });
        }

        public string? ResumeToken => _document.ResumeToken;

        public int Version => _document.Version;

        public Task UpdateResumeTokenAsync(string? resumeToken, CancellationToken cancellationToken = default)
        {
            _document.ResumeToken = resumeToken;
        }

        public Task UpdateVersionAsync(int version, CancellationToken cancellationToken = default)
        {
            _document.Version = version;
        }

        private Task SyncStateAsync(CancellationToken cancellationToken = default)
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
                        Builders<StateMongoDbDocument>.Update.Set(document => document.ResumeToken, _document.ResumeToken),
                        Builders<StateMongoDbDocument>.Update.Set(document => document.Version, _document.Version),
                        Builders<StateMongoDbDocument>.Update.Set(document => document.UpdatedAtUtc, utcNow)),
                    options: new FindOneAndUpdateOptions<StateMongoDbDocument> { IsUpsert = false, ReturnDocument = ReturnDocument.After },
                    cancellationToken: cancellationToken);
        }

        public async ValueTask DisposeAsync()
        {
        }
    }
}

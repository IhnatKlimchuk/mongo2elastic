using MongoDB.Bson;

namespace Notidar.Mongo2Elastic.States
{
    public interface IReplicationStateRepository
    {
        Task<ReplicationState?> TryLockStateAsync(
            string replicationName,
            Guid replicatorId,
            DateTime lockExpirationDateUtc,
            CancellationToken cancellationToken = default);

        Task<ReplicationState?> TryUpdateStateAsync(
            string replicationName,
            Guid replicatorId,
            DateTime lockExpirationDateUtc,
            BsonDocument? resumeToken = null,
            CancellationToken cancellationToken = default);
    }
}

using MongoDB.Bson;

namespace Notidar.Mongo2Elastic.Replication
{
    public interface IReplicationStateRepository
    {
        Task<ReplicationState?> TryLockStateAsync(
            string replicationKey,
            Guid replicatorId,
            DateTime lockExpirationDateUtc,
            CancellationToken cancellationToken = default);

        Task<ReplicationState?> TryUpdateStateAsync(
            string replicationKey,
            Guid replicatorId,
            DateTime lockExpirationDateUtc,
            BsonDocument? resumeToken = null,
            CancellationToken cancellationToken = default);
    }
}

namespace Notidar.Mongo2Elastic
{
    public interface IReplicationStateRepository
    {
        Task<ReplicationState?> TryLockStateAsync(
            Guid replicatorId,
            DateTime lockExpirationDateUtc,
            CancellationToken cancellationToken = default);

        Task TryUnlockStateAsync(
            Guid replicatorId,
            CancellationToken cancellationToken = default);

        Task<ReplicationState?> TryUpdateStateAsync(
            Guid replicatorId,
            DateTime lockExpirationDateUtc,
            int version,
            string? resumeToken = null,
            CancellationToken cancellationToken = default);
    }
}

namespace Notidar.Mongo2Elastic
{
    public class ReplicationState
    {
        public string Id { get; init; }
        public Guid? LockReplicatorId { get; set; }
        public DateTime? LockExpirationDateUtc { get; set; }
        public DateTime? UpdatedAtUtc { get; set; }
        public string? ResumeToken { get; set; }
        public int Version { get; set; }
    }
}

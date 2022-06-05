namespace Notidar.Mongo2Elastic.State
{
    public class StateMongoDbDocument
    {
        public string Id { get; set; }
        public string? ResumeToken { get; set; }
        public int Version { get; set; }
        public Guid? LockReplicatorId { get; set; }
        public DateTime? LockExpirationDateUtc { get; set; }
        public DateTime? UpdatedAtUtc { get; set; }
    }
}

using MongoDB.Bson;

namespace Notidar.Mongo2Elastic.Replication
{
    public class ReplicationState
    {
        public string ReplicationKey { get; init; }
        public Guid? ReplicatorId { get; set; }
        public DateTime? LockExpirationDateUtc { get; set; }
        public DateTime? UpdatedAtUtc { get; set; }
        public BsonDocument? ResumeToken { get; set; }
    }
}

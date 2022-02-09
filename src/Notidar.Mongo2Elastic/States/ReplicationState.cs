using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Notidar.Mongo2Elastic.States
{
    public class ReplicationState
    {
        [BsonId]
        public string ReplicationKey { get; init; }
        public Guid? ReplicatorId { get; set; }
        public DateTime? LockExpirationDateUtc { get; set; }
        public DateTime? UpdatedAtUtc { get; set; }
        public BsonDocument? ResumeToken { get; set; }
    }
}

namespace Notidar.Mongo2Elastic
{
    public class ReplicatorOptions
    {
        public string ReplicationName { get; set; }
        public TimeSpan StateUpdateDelay { get; init; }
        public TimeSpan LockTimeout { get; init; }
        public TimeSpan MaxSourceAwaitTime { get; init; }
        public int BatchSize { get; init; }
    }
}

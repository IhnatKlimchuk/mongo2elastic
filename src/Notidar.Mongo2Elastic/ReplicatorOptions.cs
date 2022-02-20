namespace Notidar.Mongo2Elastic
{
    public class ReplicatorOptions
    {
        public TimeSpan StateUpdateDelay { get; set; } = TimeSpan.FromSeconds(20);
        public TimeSpan LockTimeout { get; set; } = TimeSpan.FromSeconds(60);
    }
}

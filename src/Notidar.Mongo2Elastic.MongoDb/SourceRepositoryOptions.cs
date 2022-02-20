namespace Notidar.Mongo2Elastic.MongoDB
{
    public class SourceRepositoryOptions
    {
        public TimeSpan MaxAwaitTime { get; set; } = TimeSpan.FromSeconds(1);
        public int BatchSize { get; set; } = 1000;
    }
}

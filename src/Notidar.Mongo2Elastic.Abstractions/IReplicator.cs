namespace Notidar.Mongo2Elastic
{
    public interface IReplicator
    {
        Task StartAsync(CancellationToken cancellationToken = default);
        Task StopAsync(CancellationToken cancellationToken = default);
    }
}

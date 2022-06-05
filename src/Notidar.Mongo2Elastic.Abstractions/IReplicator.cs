namespace Notidar.Mongo2Elastic
{
    public interface IReplicator : IAsyncDisposable
    {
        Task StartAsync(CancellationToken cancellationToken = default);
        Task StopAsync(CancellationToken cancellationToken = default);
    }
}

namespace Notidar.Mongo2Elastic
{
    public interface IReplicator : IDisposable
    {
        Task ExecuteAsync(CancellationToken cancellationToken = default);
    }
}

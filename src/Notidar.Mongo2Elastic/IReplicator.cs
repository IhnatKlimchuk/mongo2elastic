namespace Notidar.Mongo2Elastic
{
    public interface IReplicator
    {
        Task ExecuteAsync(CancellationToken cancellationToken = default);
    }
}

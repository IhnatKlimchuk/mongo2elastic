namespace Notidar.Mongo2Elastic.MongoDB
{
    public interface IAsyncReplicationStream<TDocument> : IAsyncEnumerable<IEnumerable<Operation<TDocument>>>, IDisposable where TDocument : class
    {
        string GetResumeToken();
    }
}

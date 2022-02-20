namespace Notidar.Mongo2Elastic
{
    public interface IAsyncReplicationStream<TDocument, TKey> : IAsyncEnumerable<IEnumerable<Operation<TDocument, TKey>>>, IDisposable where TDocument : class
    {
        string GetResumeToken();
    }
}

namespace Notidar.Mongo2Elastic.MongoDB
{
    public interface IAsyncReplicationStream<TDocument, TKey> : IAsyncEnumerable<IEnumerable<Operation<TDocument, TKey>>>, IDisposable where TDocument : class
    {
        string GetResumeToken();
    }
}

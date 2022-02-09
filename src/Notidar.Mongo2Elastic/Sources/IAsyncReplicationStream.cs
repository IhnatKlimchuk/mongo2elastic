namespace Notidar.Mongo2Elastic.Sources
{
    public interface IAsyncReplicationStream<TSourceDocument, TKey> : IAsyncEnumerable<IEnumerable<Operation<TSourceDocument, TKey>>>, IDisposable where TSourceDocument : class
    {
        string GetResumeToken();
    }
}

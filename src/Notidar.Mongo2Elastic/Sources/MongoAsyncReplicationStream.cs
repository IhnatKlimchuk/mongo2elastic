using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace Notidar.Mongo2Elastic.Sources
{
    public class MongoAsyncReplicationStream<TSourceDocument, TKey> : IAsyncReplicationStream<TSourceDocument, TKey> where TSourceDocument : class
    {
        private IChangeStreamCursor<ChangeStreamDocument<TSourceDocument>> _changeStreamCursor;
        private Func<TSourceDocument, TKey> _getKey;
        public MongoAsyncReplicationStream(
            IChangeStreamCursor<ChangeStreamDocument<TSourceDocument>> changeStreamCursor,
            Func<TSourceDocument, TKey> getKey)
        {
            _changeStreamCursor = changeStreamCursor;
            _getKey = getKey;
        }

        public void Dispose()
        {
            _changeStreamCursor.Dispose();
        }

        public async IAsyncEnumerator<IEnumerable<Operation<TSourceDocument, TKey>>> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            while (await _changeStreamCursor.MoveNextAsync(cancellationToken))
            {
                yield return _changeStreamCursor.Current.Select(x =>
                {
                    var document = x.FullDocument ?? BsonSerializer.Deserialize<TSourceDocument>(x.DocumentKey);
                    return new Operation<TSourceDocument, TKey>
                    {
                        Type = x.OperationType switch
                        {
                            ChangeStreamOperationType.Insert => OperationType.AddOrUpdate,
                            ChangeStreamOperationType.Update => OperationType.AddOrUpdate,
                            ChangeStreamOperationType.Replace => OperationType.AddOrUpdate,
                            ChangeStreamOperationType.Delete => OperationType.Delete,
                            _ => throw new NotSupportedException()
                        },
                        Document = document,
                        Key = _getKey(document)
                    };
                });
            }
        }

        public string GetResumeToken()
        {
            return _changeStreamCursor.GetResumeToken().GetValue("_data").AsString;
        }
    }
}

using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace Notidar.Mongo2Elastic.MongoDB
{
    public class MongoAsyncReplicationStream<TSourceDocument> : IAsyncReplicationStream<TSourceDocument> where TSourceDocument : class
    {
        private readonly IChangeStreamCursor<ChangeStreamDocument<TSourceDocument>> _changeStreamCursor;
        private readonly Func<object, object> _getKey;
        public MongoAsyncReplicationStream(
            IChangeStreamCursor<ChangeStreamDocument<TSourceDocument>> changeStreamCursor)
        {
            _changeStreamCursor = changeStreamCursor;
            _getKey = BsonClassMap.LookupClassMap(typeof(TSourceDocument)).IdMemberMap.Getter;
        }

        public void Dispose()
        {
            _changeStreamCursor.Dispose();
            GC.SuppressFinalize(this);
        }

        public async IAsyncEnumerator<IEnumerable<Operation<TSourceDocument>>> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            while (await _changeStreamCursor.MoveNextAsync(cancellationToken))
            {
                yield return _changeStreamCursor.Current.Select(x =>
                {
                    var document = x.FullDocument ?? BsonSerializer.Deserialize<TSourceDocument>(x.DocumentKey);
                    return new Operation<TSourceDocument>
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

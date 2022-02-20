using MongoDB.Driver;
using System.Runtime.CompilerServices;

namespace Notidar.Mongo2Elastic.MongoDB
{
    public static class MongoDriverExtenstions
    {
        public static async IAsyncEnumerable<IEnumerable<T>> ToAsyncEnumerable<T>(
            this IAsyncCursor<T> asyncCursor,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            while (await asyncCursor.MoveNextAsync(cancellationToken))
            {
                yield return asyncCursor.Current;
            }
        }
    }
}

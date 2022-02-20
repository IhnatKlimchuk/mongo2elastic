using MongoDB.Bson.Serialization;
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

        public static async IAsyncEnumerable<IEnumerable<V>> ToAsyncEnumerable<T, V>(
            this IAsyncCursor<T> asyncCursor,
            Func<T, V> map,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            while (await asyncCursor.MoveNextAsync(cancellationToken))
            {
                yield return asyncCursor.Current.Select(map);
            }
        }

        public static async IAsyncEnumerable<T> ToFlatAsyncEnumerable<T>(
            this IAsyncCursor<T> asyncCursor,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            while (await asyncCursor.MoveNextAsync(cancellationToken))
            {
                foreach (var current in asyncCursor.Current)
                {
                    yield return current;
                }
            }
        }

        public static async IAsyncEnumerable<V> ToFlatAsyncEnumerable<T, V>(
            this IAsyncCursor<T> asyncCursor,
            Func<T, V> map,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            while (await asyncCursor.MoveNextAsync(cancellationToken))
            {
                foreach (var current in asyncCursor.Current)
                {
                    yield return map(current);
                }
            }
        }
    }
}

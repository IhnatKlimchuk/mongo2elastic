using System.Linq.Expressions;

namespace Notidar.Mongo2Elastic.MongoDB
{
    public class SourceRepositoryOptions<TDocument>
    {
        public TimeSpan MaxAwaitTime { get; set; } = TimeSpan.FromSeconds(1);
        public int BatchSize { get; set; } = 1000;
        public IReadOnlyCollection<Expression<Func<TDocument, object>>>? ProjectionExcludeFields { get; set; } = null;
    }
}

namespace Notidar.Mongo2Elastic.Sources
{
    public class Operation<TSourceDocument, TKey>
    {
        public OperationType Type { get; set; }
        public TKey Key { get; set; }
        public TSourceDocument Document { get; set; }
    }
}

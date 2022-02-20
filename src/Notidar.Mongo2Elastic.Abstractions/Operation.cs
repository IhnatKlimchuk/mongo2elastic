namespace Notidar.Mongo2Elastic
{
    public class Operation<TDocument, TKey>
    {
        public OperationType Type { get; set; }
        public TKey Key { get; set; }
        public TDocument Document { get; set; }
    }
}

namespace Notidar.Mongo2Elastic.MongoDB
{
    public class Operation<TDocument>
    {
        public OperationType Type { get; set; }
        public object Key { get; set; }
        public TDocument Document { get; set; }
    }
}

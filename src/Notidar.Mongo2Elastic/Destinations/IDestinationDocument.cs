namespace Notidar.Mongo2Elastic.Destinations
{
    public interface IDestinationDocument<TKey>
    {
        public TKey DocumentId { get; }
    }
}

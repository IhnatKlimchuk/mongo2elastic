namespace Notidar.Mongo2Elastic.Destinations
{
    public interface IVersionedDestinationDocument
    {
        public int ReplicationVersion { get; set; }
    }
}

namespace Notidar.Mongo2Elastic.Elasticsearch
{
    public interface IVersionedDocument
    {
        int Mongo2ElasticReplicationVersion { get; set; }
    }
}

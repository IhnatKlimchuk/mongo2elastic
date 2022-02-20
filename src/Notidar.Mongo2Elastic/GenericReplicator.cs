using Notidar.Mongo2Elastic.Elasticsearch;
using Notidar.Mongo2Elastic.MongoDB;

namespace Notidar.Mongo2Elastic
{
    public class GenericReplicator<TDocument, TKey> : ConvertingGenericReplicator<TDocument, TKey, TDocument> where TDocument : class
    {
        public GenericReplicator(
            IReplicationStateRepository replicationStateRepository,
            IDestinationRepository<TDocument> destinationRepository,
            ISourceRepository<TDocument, TKey> sourceRepository,
            ReplicatorOptions options) : base(replicationStateRepository, destinationRepository, sourceRepository, x => x, options)
        {
        }
    }
}
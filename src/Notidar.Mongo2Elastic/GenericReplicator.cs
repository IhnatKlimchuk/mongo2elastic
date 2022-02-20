using Notidar.Mongo2Elastic.Elasticsearch;
using Notidar.Mongo2Elastic.MongoDB;

namespace Notidar.Mongo2Elastic
{
    public class GenericReplicator<TDocument> : ConvertingGenericReplicator<TDocument, TDocument> where TDocument : class
    {
        public GenericReplicator(
            IReplicationStateRepository replicationStateRepository,
            IDestinationRepository<TDocument> destinationRepository,
            ISourceRepository<TDocument> sourceRepository,
            ReplicatorOptions options) : base(replicationStateRepository, destinationRepository, sourceRepository, x => x, options)
        {
        }
    }
}
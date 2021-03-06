using Notidar.Mongo2Elastic.Elasticsearch;
using Notidar.Mongo2Elastic.MongoDB;
using Notidar.Mongo2Elastic.State;

namespace Notidar.Mongo2Elastic.Builder
{
    public class ReplicatorBuilder<TSource, TDestination> :
        IReplicatorSourceBuilder<TSource, TDestination>,
        IReplicatorDestinationBuilder<TSource, TDestination>,
        IReplicatorStateBuilder<TSource, TDestination>,
        IReplicatorBuilder<TSource, TDestination>
        where TDestination : class
        where TSource : class
    {
        private readonly Func<TSource, TDestination> _map;

        private IStateRepository _stateRepository = null;
        private IDestinationRepository<TDestination> _destinationRepository = null;
        private ISourceRepository<TSource> _sourceRepository = null;

        public ReplicatorBuilder(Func<TSource, TDestination> map)
        {
            _map = map ?? throw new ArgumentNullException(nameof(map));
        }

        public IReplicatorDestinationBuilder<TSource, TDestination> Add(ISourceRepository<TSource> sourceRepository)
        {
            _sourceRepository = sourceRepository;
            return this;
        }

        public IReplicatorStateBuilder<TSource, TDestination> Add(IDestinationRepository<TDestination> destinationRepository)
        {
            _destinationRepository = destinationRepository;
            return this;
        }

        public IReplicatorBuilder<TSource, TDestination> Add(IStateRepository stateRepository)
        {
            _stateRepository = stateRepository;
            return this;
        }

        public IReplicator Build() => new Replicator<TSource, TDestination>(
            _stateRepository ?? throw new InvalidOperationException(),
            _destinationRepository ?? throw new InvalidOperationException(),
            _sourceRepository ?? throw new InvalidOperationException(),
            _map ?? throw new InvalidOperationException());
    }
}

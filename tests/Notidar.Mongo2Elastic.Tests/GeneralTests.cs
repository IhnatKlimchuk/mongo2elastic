using Elasticsearch.Net;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using Nest;
using Notidar.Mongo2Elastic.Destinations;
using Notidar.Mongo2Elastic.Sources;
using Notidar.Mongo2Elastic.States;
using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace Notidar.Mongo2Elastic.Tests
{
    public class GeneralTests
    {
        private readonly ServiceProvider _serviceProvider;
        private readonly IReplicator _replicator;  
        public GeneralTests()
        {
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false)
                .Build();

            _serviceProvider = new ServiceCollection()
                .Configure<ReplicatorOptions>(configuration.GetSection(nameof(ReplicatorOptions)))
                .Configure<SourceOptions>(configuration.GetSection(nameof(SourceOptions)))
                .Configure<DestinationOptions>(configuration.GetSection(nameof(DestinationOptions)))
                .Configure<StateOptions>(configuration.GetSection(nameof(StateOptions)))
                .BuildServiceProvider();

            var replicatorOptions = _serviceProvider.GetRequiredService<IOptions<ReplicatorOptions>>();
            var sourceOptions = _serviceProvider.GetRequiredService<IOptions<SourceOptions>>();
            var destinationOptions = _serviceProvider.GetRequiredService<IOptions<DestinationOptions>>();
            var stateOptions = _serviceProvider.GetRequiredService<IOptions<StateOptions>>();

            var clientSettings = MongoClientSettings.FromConnectionString(sourceOptions.Value.MongoConnectionString);
            var mongoClient = new MongoClient(clientSettings);
            var mongoDb = mongoClient.GetDatabase("test");

            var url = new Uri(destinationOptions.Value.ElasticSearchUrl);
            var pool = new SingleNodeConnectionPool(url);
            var config = new ConnectionSettings(pool)
                .DefaultMappingFor<ElasticPerson>(p => p
                .IndexName("persons")
                .IdProperty(x => x.Id))
                .EnableDebugMode()
                .PrettyJson()
                .MaximumRetries(3)
                .RequestTimeout(TimeSpan.FromSeconds(30))
                .MaxRetryTimeout(TimeSpan.FromSeconds(60));
            var elasticSearchClient = new ElasticClient(config);

            var stateCollection = mongoDb.GetCollection<ReplicationState>("replications");
            var replicationStateRepository = new MongoReplicationStateRepository(stateCollection);
            var destinationRepository = new DestinationRepository<ElasticPerson>(elasticSearchClient, "persons");
            var personsCollection = mongoDb.GetCollection<MongoPerson>("persons");
            var sourceRepository = new SourceRepository<MongoPerson>(personsCollection);

            _replicator = new GenericReplicator<MongoPerson, Guid, ElasticPerson>(
                replicationStateRepository,
                destinationRepository,
                sourceRepository,
                (x) => new ElasticPerson { Id = x.Id },
                replicatorOptions.Value);

        }

        [Fact]
        public async Task GeneralTest()
        {
            await _replicator.ExecuteAsync();
        }
    }
}
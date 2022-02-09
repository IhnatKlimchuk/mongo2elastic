using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Notidar.Mongo2Elastic.Destinations;
using Notidar.Mongo2Elastic.Sources;
using Notidar.Mongo2Elastic.States;
using Notidar.Mongo2Elastic.Tests.Fixtures.Elastic;
using Notidar.Mongo2Elastic.Tests.Fixtures.Mongo;
using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace Notidar.Mongo2Elastic.Tests
{
    [Collection("Fixture")]
    public class GenericReplicatorTests
    {
        private readonly ServiceProvider _serviceProvider;
        private readonly IReplicator _replicator;
        private readonly MongoDbFixture _mongoDbFixture;
        private readonly ElasticSearchFixture _elasticSearchFixture;

        public GenericReplicatorTests(MongoDbFixture mongoDbFixture, ElasticSearchFixture elasticSearchFixture)
        {
            _mongoDbFixture = mongoDbFixture;
            _elasticSearchFixture = elasticSearchFixture;

            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false)
                .Build();

            _serviceProvider = new ServiceCollection()
                .Configure<ReplicatorOptions>(configuration.GetSection(nameof(ReplicatorOptions)))
                .BuildServiceProvider();

            var replicatorOptions = _serviceProvider.GetRequiredService<IOptions<ReplicatorOptions>>();

            _replicator = new GenericReplicator<Fixtures.Mongo.Person, Guid, Fixtures.Elastic.Person>(
                new MongoReplicationStateRepository(_mongoDbFixture.ReplicationStateCollection),
                new DestinationRepository<Fixtures.Elastic.Person>(_elasticSearchFixture.Client, "persons"),
                new SourceRepository<Fixtures.Mongo.Person>(_mongoDbFixture.PersonCollection),
                (x) => new Fixtures.Elastic.Person { Id = x.Id },
                replicatorOptions.Value);

        }

        [Fact]
        public async Task GeneralTest()
        {
            await _replicator.ExecuteAsync();
        }
    }
}
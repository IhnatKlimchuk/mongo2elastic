using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MongoDB.Bson.Serialization;
using Notidar.Mongo2Elastic.Elasticsearch;
using Notidar.Mongo2Elastic.MongoDB;
using Notidar.Mongo2Elastic.Tests.Fixtures;
using Notidar.Mongo2Elastic.Tests.Fixtures.Elastic;
using Notidar.Mongo2Elastic.Tests.Fixtures.Mongo;
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Notidar.Mongo2Elastic.Tests
{
    [Collection("Fixture")]
    public class GenericReplicatorTests
    {
        private readonly ServiceProvider _serviceProvider;
        private readonly IReplicator _replicator;
        private readonly ReplicatorOptions _options;
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

            _options = _serviceProvider.GetRequiredService<IOptions<ReplicatorOptions>>().Value;

            _replicator = new ConvertingGenericReplicator<Fixtures.Mongo.Person, Guid, Fixtures.Elastic.Person>(
                new MongoReplicationStateRepository(_mongoDbFixture.ReplicationStateCollection, "persons"),
                new DestinationRepository<Fixtures.Elastic.Person>(_elasticSearchFixture.Client),
                new SourceRepository<Fixtures.Mongo.Person, Guid>(_mongoDbFixture.PersonCollection, TimeSpan.FromSeconds(1), x => x.Id),
                x => new Fixtures.Elastic.Person { Id = x.Id },
                _options);
        }

        [Fact]
        public async Task GenericReplicator_ReplicateAlreadExistingDocument_Success()
        {
            await _mongoDbFixture.ResetReplicationStateAsync("persons");
            await _mongoDbFixture.DeleteAllPersonsAsync();
            await _elasticSearchFixture.DeleteAllPersonsAsync();
            var mongoPersons = await _mongoDbFixture.AddNewPersonsAsync(1);
            var mongoPerson = mongoPersons.Single();

            using var cancellationTokenSource = new CancellationTokenSource();
            var task = _replicator.ExecuteAsync(cancellationTokenSource.Token);
            await Assertion.Eventually(async () =>
            {
                var elasticPerson = await _elasticSearchFixture.GetPersonOrDefaultAsync(mongoPerson.Id);
                Assert.Equal(mongoPerson.Id, elasticPerson?.Id);
            });
            cancellationTokenSource.Cancel();

            await task;
        }

        [Fact]
        public async Task GenericReplicator_ReplicateAlreadExistingDocuments_Success()
        {
            await _mongoDbFixture.ResetReplicationStateAsync("persons");
            await _mongoDbFixture.DeleteAllPersonsAsync();
            await _elasticSearchFixture.DeleteAllPersonsAsync();
            var mongoPersons = await _mongoDbFixture.AddNewPersonsAsync(10);

            using var cancellationTokenSource = new CancellationTokenSource();
            var task = _replicator.ExecuteAsync(cancellationTokenSource.Token);
            await Assertion.Eventually(async () =>
            {
                var count = await _elasticSearchFixture.CountPersonsAsync();
                Assert.Equal(mongoPersons.Count, count);
            });
            cancellationTokenSource.Cancel();

            await task;
        }

        [Fact]
        public async Task GenericReplicator_ReplicateAddedDocument_Success()
        {
            await _mongoDbFixture.ResetReplicationStateAsync("persons");
            await _mongoDbFixture.DeleteAllPersonsAsync();
            await _elasticSearchFixture.DeleteAllPersonsAsync();

            using var cancellationTokenSource = new CancellationTokenSource();
            var task = _replicator.ExecuteAsync(cancellationTokenSource.Token);

            await Assertion.Eventually(async () =>
            {
                var state = await _mongoDbFixture.GetReplicationStateOrDefaultAsync("persons");
                Assert.NotNull(state?.ResumeToken);
            });

            var mongoPersons = await _mongoDbFixture.AddNewPersonsAsync(1);
            var mongoPerson = mongoPersons.Single();

            await Assertion.Eventually(async () =>
            {
                var elasticPerson = await _elasticSearchFixture.GetPersonOrDefaultAsync(mongoPerson.Id);
                Assert.Equal(mongoPerson.Id, elasticPerson?.Id);
            });
            cancellationTokenSource.Cancel();

            await task;
        }

        [Fact]
        public async Task GenericReplicator_ReplicateDeletedDocument_Success()
        {
            await _mongoDbFixture.ResetReplicationStateAsync("persons");
            await _mongoDbFixture.DeleteAllPersonsAsync();
            await _elasticSearchFixture.DeleteAllPersonsAsync();
            var mongoPersons = await _mongoDbFixture.AddNewPersonsAsync(1);
            var mongoPerson = mongoPersons.Single();

            using var cancellationTokenSource = new CancellationTokenSource();
            var task = _replicator.ExecuteAsync(cancellationTokenSource.Token);

            await Assertion.Eventually(async () =>
            {
                var elasticPerson = await _elasticSearchFixture.GetPersonOrDefaultAsync(mongoPerson.Id);
                Assert.Equal(mongoPerson.Id, elasticPerson?.Id);
            });

            await _mongoDbFixture.DeletePersonAsync(mongoPerson.Id);

            await Assertion.Eventually(async () =>
            {
                var elasticPerson = await _elasticSearchFixture.GetPersonOrDefaultAsync(mongoPerson.Id);
                Assert.Null(elasticPerson?.Id);
            });
            cancellationTokenSource.Cancel();

            await task;
        }
    }
}
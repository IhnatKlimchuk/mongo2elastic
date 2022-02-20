using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Notidar.Mongo2Elastic.Elasticsearch;
using Notidar.Mongo2Elastic.MongoDB;
using Notidar.Mongo2Elastic.Tests.Fixtures;
using Notidar.Mongo2Elastic.Tests.Fixtures.Elasticsearch;
using Notidar.Mongo2Elastic.Tests.Fixtures.MongoDB;
using Notidar.Mongo2Elastic.Tests.Fixtures.MongoDB.Models;
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Notidar.Mongo2Elastic.Tests.Features
{
    [Collection("Fixture")]
    public class MongoDbCompositeIdTests
    {
        private readonly ServiceProvider _serviceProvider;
        private readonly IReplicator _replicator;
        private readonly ReplicatorOptions _options;
        private readonly MongoDbFixture _mongoDbFixture;
        private readonly ElasticSearchFixture _elasticSearchFixture;

        public MongoDbCompositeIdTests(MongoDbFixture mongoDbFixture, ElasticSearchFixture elasticSearchFixture)
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

            _replicator = new ConvertingGenericReplicator<Fixtures.MongoDB.Models.CompositeIdPerson, Fixtures.Elasticsearch.Models.Person>(
                new MongoReplicationStateRepository(_mongoDbFixture.ReplicationStateCollection, "composite-id-persons"),
                new DestinationRepository<Fixtures.Elasticsearch.Models.Person>(_elasticSearchFixture.Client),
                new SourceRepository<Fixtures.MongoDB.Models.CompositeIdPerson>(_mongoDbFixture.CompositeIdPersonCollection, TimeSpan.FromSeconds(1)),
                Fixtures.Elasticsearch.Models.Person.FromMongoCompositeIdPerson,
                _options);
        }

        [Fact]
        public async Task GenericReplicator_ReplicateAlreadExistingDocument_WithCompositeId_Success()
        {
            await _mongoDbFixture.ResetReplicationStateAsync("composite-id-persons");
            await _mongoDbFixture.DeleteAllCompositeIdPersonsAsync();
            await _elasticSearchFixture.DeleteAllPersonsAsync();
            var mongoPersons = await _mongoDbFixture.AddNewCompositeIdPersonsAsync(1);
            var mongoPerson = mongoPersons.Single();

            using var cancellationTokenSource = new CancellationTokenSource();
            var task = _replicator.ExecuteAsync(cancellationTokenSource.Token);
            await Assertion.Eventually(async () =>
            {
                var elasticPerson = await _elasticSearchFixture.GetPersonOrDefaultAsync(mongoPerson.Id.ToString());
                Assert.Equal(mongoPerson.Id.ToString(), elasticPerson?.Id);
            });
            cancellationTokenSource.Cancel();

            await task;
        }

        [Fact]
        public async Task GenericReplicator_ReplicateAddedDocument_WithCompositeId_Success()
        {
            await _mongoDbFixture.ResetReplicationStateAsync("composite-id-persons");
            await _mongoDbFixture.DeleteAllCompositeIdPersonsAsync();
            await _elasticSearchFixture.DeleteAllPersonsAsync();

            using var cancellationTokenSource = new CancellationTokenSource();
            var task = _replicator.ExecuteAsync(cancellationTokenSource.Token);

            await Assertion.Eventually(async () =>
            {
                var state = await _mongoDbFixture.GetReplicationStateOrDefaultAsync("composite-id-persons");
                Assert.NotNull(state?.ResumeToken);
            });

            var mongoPersons = await _mongoDbFixture.AddNewCompositeIdPersonsAsync(1);
            var mongoPerson = mongoPersons.Single();

            await Assertion.Eventually(async () =>
            {
                var elasticPerson = await _elasticSearchFixture.GetPersonOrDefaultAsync(mongoPerson.Id.ToString());
                Assert.Equal(mongoPerson.Id.ToString(), elasticPerson?.Id);
            });
            cancellationTokenSource.Cancel();

            await task;
        }

        [Fact]
        public async Task GenericReplicator_ReplicateDeletedDocument_WithCompositeId_Success()
        {
            await _mongoDbFixture.ResetReplicationStateAsync("composite-id-persons");
            await _mongoDbFixture.DeleteAllCompositeIdPersonsAsync();
            await _elasticSearchFixture.DeleteAllPersonsAsync();
            var mongoPersons = await _mongoDbFixture.AddNewCompositeIdPersonsAsync(1);
            var mongoPerson = mongoPersons.Single();

            using var cancellationTokenSource = new CancellationTokenSource();
            var task = _replicator.ExecuteAsync(cancellationTokenSource.Token);

            await Assertion.Eventually(async () =>
            {
                var elasticPerson = await _elasticSearchFixture.GetPersonOrDefaultAsync(mongoPerson.Id.ToString());
                Assert.Equal(mongoPerson.Id.ToString(), elasticPerson?.Id);
            });

            await _mongoDbFixture.DeleteCompositeIdPersonAsync(mongoPerson.Id);

            await Assertion.Eventually(async () =>
            {
                var elasticPerson = await _elasticSearchFixture.GetPersonOrDefaultAsync(mongoPerson.Id.ToString());
                Assert.Null(elasticPerson?.Id);
            });
            cancellationTokenSource.Cancel();

            await task;
        }

        [Fact]
        public async Task GenericReplicator_ReplicateDocumentMultipleUpdates_WithCompositeId_Success()
        {
            await _mongoDbFixture.ResetReplicationStateAsync("composite-id-persons");
            await _mongoDbFixture.DeleteAllCompositeIdPersonsAsync();
            await _elasticSearchFixture.DeleteAllPersonsAsync();
            var mongoPersons = await _mongoDbFixture.AddNewCompositeIdPersonsAsync(1);
            var mongoPerson = mongoPersons.Single();

            using var cancellationTokenSource = new CancellationTokenSource();
            var task = _replicator.ExecuteAsync(cancellationTokenSource.Token);

            await Assertion.Eventually(async () =>
            {
                var elasticPerson = await _elasticSearchFixture.GetPersonOrDefaultAsync(mongoPerson.Id.ToString());
                Assert.Equal(mongoPerson.Id.ToString(), elasticPerson?.Id);
            });

            CompositeIdPerson latestPerson = null;
            for (int i = 0; i < 10; i++)
            {
                latestPerson = await _mongoDbFixture.UpdateCompositeIdPersonAsync(mongoPerson.Id);
            }
            
            await Assertion.Eventually(async () =>
            {
                var elasticPerson = await _elasticSearchFixture.GetPersonOrDefaultAsync(mongoPerson.Id.ToString());
                Assert.Equal(latestPerson.Id.ToString(), elasticPerson?.Id);
                Assert.Equal(latestPerson.Email, elasticPerson?.Email);
                Assert.Equal(latestPerson.FirstName, elasticPerson?.FirstName);
                Assert.Equal(latestPerson.LastName, elasticPerson?.LastName);
                Assert.Equal(latestPerson.UserName, elasticPerson?.UserName);
            });
            cancellationTokenSource.Cancel();

            await task;
        }
    }
}
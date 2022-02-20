using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Notidar.Mongo2Elastic.Elasticsearch;
using Notidar.Mongo2Elastic.MongoDB;
using Notidar.Mongo2Elastic.Tests.Fixtures;
using Notidar.Mongo2Elastic.Tests.Fixtures.Elasticsearch;
using Notidar.Mongo2Elastic.Tests.Fixtures.MongoDB;
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Notidar.Mongo2Elastic.Tests.Features
{
    public class GenericReplicatorTests : TestBase
    {
        private readonly IReplicator _replicator;

        public GenericReplicatorTests(MongoDbFixture mongoDbFixture, ElasticsearchFixture elasticsearchFixture) : base(mongoDbFixture, elasticsearchFixture)
        {
            var options = ServiceProvider.GetRequiredService<IOptions<ReplicatorOptions>>().Value;

            _replicator = new ConvertingGenericReplicator<Fixtures.MongoDB.Models.Person, Fixtures.Elasticsearch.Models.Person>(
                new MongoReplicationStateRepository(MongoDbFixture.ReplicationStateCollection, "persons"),
                new DestinationRepository<Fixtures.Elasticsearch.Models.Person>(ElasticsearchFixture.Client),
                new SourceRepository<Fixtures.MongoDB.Models.Person>(MongoDbFixture.PersonCollection, TimeSpan.FromSeconds(1)),
                Fixtures.Elasticsearch.Models.Person.FromMongoPerson,
                options);
        }

        [Fact]
        public async Task GenericReplicator_ReplicateAlreadExistingDocument_Success()
        {
            await MongoDbFixture.ResetReplicationStateAsync("persons");
            await MongoDbFixture.DeleteAllPersonsAsync();
            await ElasticsearchFixture.DeleteAllPersonsAsync();
            var mongoPersons = await MongoDbFixture.AddNewPersonsAsync(1);
            var mongoPerson = mongoPersons.Single();

            using var cancellationTokenSource = new CancellationTokenSource();
            var task = _replicator.ExecuteAsync(cancellationTokenSource.Token);
            await Assertion.Eventually(async () =>
            {
                var elasticPerson = await ElasticsearchFixture.GetPersonOrDefaultAsync(mongoPerson.Id.ToString());
                Assert.Equal(mongoPerson.Id.ToString(), elasticPerson?.Id);
            });
            cancellationTokenSource.Cancel();

            await task;
        }

        [Fact]
        public async Task GenericReplicator_ReplicateAlreadExistingDocuments_Success()
        {
            await MongoDbFixture.ResetReplicationStateAsync("persons");
            await MongoDbFixture.DeleteAllPersonsAsync();
            await ElasticsearchFixture.DeleteAllPersonsAsync();
            var mongoPersons = await MongoDbFixture.AddNewPersonsAsync(10);

            using var cancellationTokenSource = new CancellationTokenSource();
            var task = _replicator.ExecuteAsync(cancellationTokenSource.Token);
            await Assertion.Eventually(async () =>
            {
                var count = await ElasticsearchFixture.CountPersonsAsync();
                Assert.Equal(mongoPersons.Count, count);
            });
            cancellationTokenSource.Cancel();

            await task;
        }

        [Fact]
        public async Task GenericReplicator_ReplicateAddedDocument_Success()
        {
            await MongoDbFixture.ResetReplicationStateAsync("persons");
            await MongoDbFixture.DeleteAllPersonsAsync();
            await ElasticsearchFixture.DeleteAllPersonsAsync();

            using var cancellationTokenSource = new CancellationTokenSource();
            var task = _replicator.ExecuteAsync(cancellationTokenSource.Token);

            await Assertion.Eventually(async () =>
            {
                var state = await MongoDbFixture.GetReplicationStateOrDefaultAsync("persons");
                Assert.NotNull(state?.ResumeToken);
            });

            var mongoPersons = await MongoDbFixture.AddNewPersonsAsync(1);
            var mongoPerson = mongoPersons.Single();

            await Assertion.Eventually(async () =>
            {
                var elasticPerson = await ElasticsearchFixture.GetPersonOrDefaultAsync(mongoPerson.Id.ToString());
                Assert.Equal(mongoPerson.Id.ToString(), elasticPerson?.Id);
            });
            cancellationTokenSource.Cancel();

            await task;
        }

        [Fact]
        public async Task GenericReplicator_ReplicateDeletedDocument_Success()
        {
            await MongoDbFixture.ResetReplicationStateAsync("persons");
            await MongoDbFixture.DeleteAllPersonsAsync();
            await ElasticsearchFixture.DeleteAllPersonsAsync();
            var mongoPersons = await MongoDbFixture.AddNewPersonsAsync(1);
            var mongoPerson = mongoPersons.Single();

            using var cancellationTokenSource = new CancellationTokenSource();
            var task = _replicator.ExecuteAsync(cancellationTokenSource.Token);

            await Assertion.Eventually(async () =>
            {
                var elasticPerson = await ElasticsearchFixture.GetPersonOrDefaultAsync(mongoPerson.Id.ToString());
                Assert.Equal(mongoPerson.Id.ToString(), elasticPerson?.Id);
            });

            await MongoDbFixture.DeletePersonAsync(mongoPerson.Id);

            await Assertion.Eventually(async () =>
            {
                var elasticPerson = await ElasticsearchFixture.GetPersonOrDefaultAsync(mongoPerson.Id.ToString());
                Assert.Null(elasticPerson?.Id);
            });
            cancellationTokenSource.Cancel();

            await task;
        }
    }
}
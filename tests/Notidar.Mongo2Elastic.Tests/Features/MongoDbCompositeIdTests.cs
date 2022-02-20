using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Notidar.Mongo2Elastic.Elasticsearch;
using Notidar.Mongo2Elastic.MongoDB;
using Notidar.Mongo2Elastic.Tests.Fixtures;
using Notidar.Mongo2Elastic.Tests.Fixtures.Elasticsearch;
using Notidar.Mongo2Elastic.Tests.Fixtures.MongoDB;
using Notidar.Mongo2Elastic.Tests.Fixtures.MongoDB.Models;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Notidar.Mongo2Elastic.Tests.Features
{
    public class MongoDbCompositeIdTests : TestBase
    {
        private readonly IReplicator _replicator;

        public MongoDbCompositeIdTests(MongoDbFixture mongoDbFixture, ElasticsearchFixture elasticsearchFixture) : base(mongoDbFixture, elasticsearchFixture)
        {
            var options = ServiceProvider.GetRequiredService<IOptions<ReplicatorOptions>>().Value;

            _replicator = new Replicator<Fixtures.MongoDB.Models.CompositeIdPerson, Fixtures.Elasticsearch.Models.ElasticPerson>(
                new MongoReplicationStateRepository(MongoDbFixture.ReplicationStateCollection, "composite-id-persons"),
                new DestinationRepository<Fixtures.Elasticsearch.Models.ElasticPerson>(ElasticsearchFixture.Client, new DestinationRepositoryOptions { }),
                new SourceRepository<Fixtures.MongoDB.Models.CompositeIdPerson>(MongoDbFixture.CompositeIdPersonCollection, new SourceRepositoryOptions { }),
                Fixtures.Elasticsearch.Models.ElasticPerson.FromMongoCompositeIdPerson,
                options);
        }

        [Fact]
        public async Task GenericReplicator_ReplicateAlreadExistingDocument_WithCompositeId_Success()
        {
            await MongoDbFixture.ResetReplicationStateAsync("composite-id-persons");
            await MongoDbFixture.DeleteAllCompositeIdPersonsAsync();
            await ElasticsearchFixture.DeleteAllPersonsAsync();
            var mongoPersons = await MongoDbFixture.AddNewCompositeIdPersonsAsync(1);
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
        public async Task GenericReplicator_ReplicateAddedDocument_WithCompositeId_Success()
        {
            await MongoDbFixture.ResetReplicationStateAsync("composite-id-persons");
            await MongoDbFixture.DeleteAllCompositeIdPersonsAsync();
            await ElasticsearchFixture.DeleteAllPersonsAsync();

            using var cancellationTokenSource = new CancellationTokenSource();
            var task = _replicator.ExecuteAsync(cancellationTokenSource.Token);

            await Assertion.Eventually(async () =>
            {
                var state = await MongoDbFixture.GetReplicationStateOrDefaultAsync("composite-id-persons");
                Assert.NotNull(state?.ResumeToken);
            });

            var mongoPersons = await MongoDbFixture.AddNewCompositeIdPersonsAsync(1);
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
        public async Task GenericReplicator_ReplicateDeletedDocument_WithCompositeId_Success()
        {
            await MongoDbFixture.ResetReplicationStateAsync("composite-id-persons");
            await MongoDbFixture.DeleteAllCompositeIdPersonsAsync();
            await ElasticsearchFixture.DeleteAllPersonsAsync();
            var mongoPersons = await MongoDbFixture.AddNewCompositeIdPersonsAsync(1);
            var mongoPerson = mongoPersons.Single();

            using var cancellationTokenSource = new CancellationTokenSource();
            var task = _replicator.ExecuteAsync(cancellationTokenSource.Token);

            await Assertion.Eventually(async () =>
            {
                var elasticPerson = await ElasticsearchFixture.GetPersonOrDefaultAsync(mongoPerson.Id.ToString());
                Assert.Equal(mongoPerson.Id.ToString(), elasticPerson?.Id);
            });

            await MongoDbFixture.DeleteCompositeIdPersonAsync(mongoPerson.Id);

            await Assertion.Eventually(async () =>
            {
                var elasticPerson = await ElasticsearchFixture.GetPersonOrDefaultAsync(mongoPerson.Id.ToString());
                Assert.Null(elasticPerson?.Id);
            });
            cancellationTokenSource.Cancel();

            await task;
        }

        [Fact]
        public async Task GenericReplicator_ReplicateDocumentMultipleUpdates_WithCompositeId_Success()
        {
            await MongoDbFixture.ResetReplicationStateAsync("composite-id-persons");
            await MongoDbFixture.DeleteAllCompositeIdPersonsAsync();
            await ElasticsearchFixture.DeleteAllPersonsAsync();
            var mongoPersons = await MongoDbFixture.AddNewCompositeIdPersonsAsync(1);
            var mongoPerson = mongoPersons.Single();

            using var cancellationTokenSource = new CancellationTokenSource();
            var task = _replicator.ExecuteAsync(cancellationTokenSource.Token);

            await Assertion.Eventually(async () =>
            {
                var elasticPerson = await ElasticsearchFixture.GetPersonOrDefaultAsync(mongoPerson.Id.ToString());
                Assert.Equal(mongoPerson.Id.ToString(), elasticPerson?.Id);
            });

            CompositeIdPerson latestPerson = null;
            for (int i = 0; i < 10; i++)
            {
                latestPerson = await MongoDbFixture.UpdateCompositeIdPersonAsync(mongoPerson.Id);
            }
            
            await Assertion.Eventually(async () =>
            {
                var elasticPerson = await ElasticsearchFixture.GetPersonOrDefaultAsync(mongoPerson.Id.ToString());
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
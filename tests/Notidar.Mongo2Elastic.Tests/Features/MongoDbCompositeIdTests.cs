using Notidar.Mongo2Elastic.Builder;
using Notidar.Mongo2Elastic.Elasticsearch.Builder;
using Notidar.Mongo2Elastic.MongoDB.Builder;
using Notidar.Mongo2Elastic.Tests.Fixtures;
using Notidar.Mongo2Elastic.Tests.Fixtures.Elasticsearch;
using Notidar.Mongo2Elastic.Tests.Fixtures.Elasticsearch.Models;
using Notidar.Mongo2Elastic.Tests.Fixtures.MongoDB;
using Notidar.Mongo2Elastic.Tests.Fixtures.MongoDB.Models;
using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Notidar.Mongo2Elastic.Tests.Features
{
    public class MongoDbCompositeIdTests : TestBase
    {
        private readonly IReplicator _replicator;

        public MongoDbCompositeIdTests(MongoDbFixture mongoDbFixture, ElasticsearchFixture elasticsearchFixture) : base(mongoDbFixture, elasticsearchFixture)
        {
            _replicator = ReplicationBuilder
                .For<MongoCompositeIdPerson, ElasticPerson>(ElasticPerson.FromMongoCompositeIdPerson, c => {
                    c.StateUpdateDelay = TimeSpan.FromSeconds(1);
                    c.LockTimeout = TimeSpan.FromSeconds(5);
                })
                .FromMongoDb(MongoDbFixture.CompositeIdPersonCollection, c => {
                    c.MaxAwaitTime = TimeSpan.FromSeconds(1);
                    c.BatchSize = 1000;
                })
                .ToElasticsearchWithReset(ElasticsearchFixture.Client, c => {
                    c.Refresh = global::Elasticsearch.Net.Refresh.True;
                })
                .WithMongoDbState(MongoDbFixture.ReplicationStateCollection, "composite-id-persons")
                .Build();
        }

        [Fact]
        public async Task GenericReplicator_ReplicateAlreadExistingDocument_WithCompositeId_Success()
        {
            await MongoDbFixture.ResetReplicationStateAsync("composite-id-persons");
            await MongoDbFixture.DeleteAllCompositeIdPersonsAsync();
            await ElasticsearchFixture.DeleteAllPersonsAsync();
            var mongoPersons = await MongoDbFixture.AddNewCompositeIdPersonsAsync(1);
            var mongoPerson = mongoPersons.Single();

            await WithReplicationRunning(_replicator, async () =>
            {
                await Assertion.Eventually(async () =>
                {
                    var elasticPerson = await ElasticsearchFixture.GetPersonOrDefaultAsync(mongoPerson.Id.ToString());
                    Assert.Equal(mongoPerson.Id.ToString(), elasticPerson?.Id);
                });
            });
        }

        [Fact]
        public async Task GenericReplicator_ReplicateAddedDocument_WithCompositeId_Success()
        {
            await MongoDbFixture.ResetReplicationStateAsync("composite-id-persons");
            await MongoDbFixture.DeleteAllCompositeIdPersonsAsync();
            await ElasticsearchFixture.DeleteAllPersonsAsync();

            await WithReplicationRunning(_replicator, async () =>
            {
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
            });
        }

        [Fact]
        public async Task GenericReplicator_ReplicateDeletedDocument_WithCompositeId_Success()
        {
            await MongoDbFixture.ResetReplicationStateAsync("composite-id-persons");
            await MongoDbFixture.DeleteAllCompositeIdPersonsAsync();
            await ElasticsearchFixture.DeleteAllPersonsAsync();
            var mongoPersons = await MongoDbFixture.AddNewCompositeIdPersonsAsync(1);
            var mongoPerson = mongoPersons.Single();

            await WithReplicationRunning(_replicator, async () =>
            {
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
            });
        }

        [Fact]
        public async Task GenericReplicator_ReplicateDocumentMultipleUpdates_WithCompositeId_Success()
        {
            await MongoDbFixture.ResetReplicationStateAsync("composite-id-persons");
            await MongoDbFixture.DeleteAllCompositeIdPersonsAsync();
            await ElasticsearchFixture.DeleteAllPersonsAsync();
            var mongoPersons = await MongoDbFixture.AddNewCompositeIdPersonsAsync(1);
            var mongoPerson = mongoPersons.Single();

            await WithReplicationRunning(_replicator, async () =>
            {
                await Assertion.Eventually(async () =>
                {
                    var elasticPerson = await ElasticsearchFixture.GetPersonOrDefaultAsync(mongoPerson.Id.ToString());
                    Assert.Equal(mongoPerson.Id.ToString(), elasticPerson?.Id);
                });

                MongoCompositeIdPerson latestPerson = null;
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
            });
        }
    }
}
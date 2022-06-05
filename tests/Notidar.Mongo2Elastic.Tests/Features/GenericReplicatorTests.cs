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
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Notidar.Mongo2Elastic.Tests.Features
{
    [Collection("Integration")]
    public sealed class GenericReplicatorTests : TestBase, IAsyncDisposable
    {
        private IReplicator _replicator;

        public GenericReplicatorTests(MongoDbFixture mongoDbFixture, ElasticsearchFixture elasticsearchFixture) : base(mongoDbFixture, elasticsearchFixture)
        {
            _replicator = ReplicationBuilder
                .For<MongoPerson, ElasticPerson>(ElasticPerson.FromMongoPerson)
                .FromMongoDb(MongoDbFixture.PersonCollection)
                .ToElasticsearchWithReset(ElasticsearchFixture.Client)
                .WithMongoDbState(MongoDbFixture.ReplicationStateCollection, "persons")
                .Build();
        }

        [Fact(Skip = "TEST")]
        public async Task GenericReplicator_ReplicateAlreadExistingDocument_Success()
        {
            await MongoDbFixture.ResetReplicationStateAsync("persons");
            await MongoDbFixture.DeleteAllPersonsAsync();
            await ElasticsearchFixture.DeleteAllPersonsAsync();
            var mongoPersons = await MongoDbFixture.AddNewPersonsAsync(1);
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
        public async Task GenericReplicator_ReplicateAlreadExistingDocuments_Success()
        {
            await MongoDbFixture.ResetReplicationStateAsync("persons");
            await MongoDbFixture.DeleteAllPersonsAsync();
            await ElasticsearchFixture.DeleteAllPersonsAsync();
            var mongoPersons = await MongoDbFixture.AddNewPersonsAsync(10);

            await WithReplicationRunning(_replicator, async () =>
            {
                await Assertion.Eventually(async () =>
                {
                    var count = await ElasticsearchFixture.CountPersonsAsync();
                    Assert.Equal(mongoPersons.Count, count);
                });
            });
        }

        [Fact(Skip = "TEST")]
        public async Task GenericReplicator_ReplicateAddedDocument_Success()
        {
            await MongoDbFixture.ResetReplicationStateAsync("persons");
            await MongoDbFixture.DeleteAllPersonsAsync();
            await ElasticsearchFixture.DeleteAllPersonsAsync();

            await WithReplicationRunning(_replicator, async () =>
            {
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
            });
        }

        [Fact(Skip = "TEST")]
        public async Task GenericReplicator_ReplicateDeletedDocument_Success()
        {
            await MongoDbFixture.ResetReplicationStateAsync("persons");
            await MongoDbFixture.DeleteAllPersonsAsync();
            await ElasticsearchFixture.DeleteAllPersonsAsync();
            var mongoPersons = await MongoDbFixture.AddNewPersonsAsync(1);
            var mongoPerson = mongoPersons.Single();

            await WithReplicationRunning(_replicator, async () =>
            {
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
            });
        }

        public async ValueTask DisposeAsync()
        {
            if (_replicator is not null)
            {
                await _replicator.DisposeAsync().ConfigureAwait(false);
            }

            _replicator = null;
        }
    }
}
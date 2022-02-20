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
    public class GenericReplicatorTests : TestBase
    {
        private readonly IReplicator _replicator;

        public GenericReplicatorTests(MongoDbFixture mongoDbFixture, ElasticsearchFixture elasticsearchFixture) : base(mongoDbFixture, elasticsearchFixture)
        {
            _replicator = ReplicationBuilder
                .For<MongoPerson, ElasticPerson>(ElasticPerson.FromMongoPerson, c => {
                    c.StateUpdateDelay = TimeSpan.FromSeconds(1);
                    c.LockTimeout = TimeSpan.FromSeconds(5);
                })
                .FromMongoDb(MongoDbFixture.PersonCollection, c => {
                    c.MaxAwaitTime = TimeSpan.FromSeconds(1);
                    c.BatchSize = 1000;
                })
                .ToElasticsearchWithReset(ElasticsearchFixture.Client, c => {
                    c.Refresh = global::Elasticsearch.Net.Refresh.True;
                })
                .WithMongoDbState(MongoDbFixture.ReplicationStateCollection, "persons")
                .Build();
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
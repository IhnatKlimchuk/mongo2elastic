using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Notidar.Mongo2Elastic.Tests.Fixtures.Elasticsearch;
using Notidar.Mongo2Elastic.Tests.Fixtures.MongoDB;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Notidar.Mongo2Elastic.Tests.Features
{
    [Collection("Fixture")]
    public abstract class TestBase
    {
        protected readonly IServiceProvider ServiceProvider;
        protected readonly IConfiguration Configuration;
        protected readonly MongoDbFixture MongoDbFixture;
        protected readonly ElasticsearchFixture ElasticsearchFixture;

        public TestBase(
            MongoDbFixture mongoDbFixture,
            ElasticsearchFixture elasticsearchFixture)
        {
            MongoDbFixture = mongoDbFixture;
            ElasticsearchFixture = elasticsearchFixture;

            Configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false)
                .Build();

            ServiceProvider = new ServiceCollection()
                .BuildServiceProvider();
        }

        protected static async Task WithReplicationRunning(IReplicator replicator, Func<Task> action)
        {
            await replicator.StartAsync();

            await action();

            await replicator.StopAsync();
        }
    }
}

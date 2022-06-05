using Notidar.Mongo2Elastic.Tests.Fixtures.Elasticsearch;
using Notidar.Mongo2Elastic.Tests.Fixtures.MongoDB;
using Xunit;

namespace Notidar.Mongo2Elastic.Tests.Fixtures
{
    [CollectionDefinition("Integration", DisableParallelization = true)]
    public class FixtureCollection :
        ICollectionFixture<MongoDbFixture>,
        ICollectionFixture<ElasticsearchFixture>
    {
    }
}

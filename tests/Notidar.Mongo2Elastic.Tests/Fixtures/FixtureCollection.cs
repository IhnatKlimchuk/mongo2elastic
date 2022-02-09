using Notidar.Mongo2Elastic.Tests.Fixtures.Elastic;
using Notidar.Mongo2Elastic.Tests.Fixtures.Mongo;
using Xunit;

namespace Notidar.Mongo2Elastic.Tests.Fixtures
{
    [CollectionDefinition("Fixture", DisableParallelization = true)]
    public class FixtureCollection :
        ICollectionFixture<MongoDbFixture>,
        ICollectionFixture<ElasticSearchFixture>
    {
    }
}

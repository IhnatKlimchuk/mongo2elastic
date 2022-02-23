using MongoDB.Driver;
using Notidar.Mongo2Elastic.MongoDB;
using Notidar.Mongo2Elastic.Tests.Fixtures.Elasticsearch;
using Notidar.Mongo2Elastic.Tests.Fixtures.MongoDB;
using Notidar.Mongo2Elastic.Tests.Fixtures.MongoDB.Models;
using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Notidar.Mongo2Elastic.Tests.Features
{
    public class SourceRepositoryTests : TestBase
    {
        public SourceRepositoryTests(MongoDbFixture mongoDbFixture, ElasticsearchFixture elasticsearchFixture) : base(mongoDbFixture, elasticsearchFixture)
        {
        }

        [Fact]
        public async Task SourceRepository_GetDocumentsAsync_UseExcludeProjection()
        {
            var sourceRepository = new SourceRepository<MongoPerson>(
                MongoDbFixture.PersonCollection,
                new SourceRepositoryOptions { BatchSize = 1000, MaxAwaitTime = TimeSpan.FromSeconds(30) },
                x => x.FirstName,
                x => x.LastName);

            await MongoDbFixture.DeleteAllPersonsAsync();
            await MongoDbFixture.AddNewPersonsAsync(1);

            var personsCursor = await sourceRepository.GetDocumentsAsync();
            await foreach (var personBatch in personsCursor)
            {
                foreach (var person in personBatch)
                {
                    Assert.Null(person.FirstName);
                    Assert.Null(person.LastName);
                    Assert.NotNull(person.UserName);
                    Assert.NotNull(person.Email);
                    Assert.NotEqual(Guid.Empty, person.Id);
                }
            }
        }

        [Fact]
        public async Task SourceRepository_TryGetStreamAsync_UseExcludeProjection()
        {
            var sourceRepository = new SourceRepository<MongoPerson>(
                MongoDbFixture.PersonCollection,
                new SourceRepositoryOptions { BatchSize = 1000, MaxAwaitTime = TimeSpan.FromSeconds(30) },
                x => x.FirstName,
                x => x.LastName);

            await MongoDbFixture.DeleteAllPersonsAsync();
            
            using var personsStreamCursor = await sourceRepository.TryGetStreamAsync();
            
            await MongoDbFixture.AddNewPersonsAsync(1);

            await foreach (var personBatch in personsStreamCursor)
            {
                var person = personBatch.SingleOrDefault();
                if (person != null)
                {
                    Assert.Null(person.Document.FirstName);
                    Assert.Null(person.Document.LastName);
                    Assert.NotNull(person.Document.UserName);
                    Assert.NotNull(person.Document.Email);
                    Assert.NotEqual(Guid.Empty, person.Document.Id);

                    Assert.Equal(OperationType.AddOrUpdate, person.Type);
                    Assert.Equal(person.Document.Id, person.Key);
                    break;
                }
            }
        }
    }
}

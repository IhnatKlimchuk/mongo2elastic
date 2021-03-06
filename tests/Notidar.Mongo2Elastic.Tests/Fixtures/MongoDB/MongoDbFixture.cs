using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using Notidar.Mongo2Elastic.State;
using Notidar.Mongo2Elastic.Tests.Fixtures.MongoDB.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static Notidar.Mongo2Elastic.Tests.Fixtures.MongoDB.Models.MongoCompositeIdPerson;

namespace Notidar.Mongo2Elastic.Tests.Fixtures.MongoDB
{
    public class MongoDbFixture : IDisposable
    {
        private readonly ServiceProvider _serviceProvider;

        public IMongoClient Client { get; private set; }
        public IMongoDatabase Database { get; private set; }
        public IMongoCollection<MongoPerson> PersonCollection { get; private set; }
        public IMongoCollection<MongoCompositeIdPerson> CompositeIdPersonCollection { get; private set; }
        public IMongoCollection<StateMongoDbDocument> ReplicationStateCollection { get; private set; }

        public MongoDbFixture()
        {
            var configuration = new ConfigurationBuilder()
                  .SetBasePath(Directory.GetCurrentDirectory())
                  .AddJsonFile("appsettings.json", optional: false)
                  .Build();

            _serviceProvider = new ServiceCollection()
                .Configure<SourceOptions>(configuration.GetSection(nameof(SourceOptions)))
                .BuildServiceProvider();

            var sourceOptions = _serviceProvider.GetRequiredService<IOptions<SourceOptions>>();

            Client = new MongoClient(MongoClientSettings.FromConnectionString(sourceOptions.Value.MongoConnectionString));
            Database = Client.GetDatabase(sourceOptions.Value.MongoDatabase);
            PersonCollection = Database.GetCollection<MongoPerson>("persons");
            CompositeIdPersonCollection = Database.GetCollection<MongoCompositeIdPerson>("composite-id-persons");
            ReplicationStateCollection = Database.GetCollection<StateMongoDbDocument>("replications");
        }

        public async Task<ICollection<MongoPerson>> AddNewPersonsAsync(int count = 10, CancellationToken cancellationToken = default)
        {
            var personsToAdd = MongoPerson.Generate(count);
            await PersonCollection.InsertManyAsync(personsToAdd, cancellationToken: cancellationToken);
            return personsToAdd;
        }

        public async Task<MongoPerson> UpdatePersonAsync(Guid personId, CancellationToken cancellationToken = default)
        {
            var updatedPersons = MongoPerson.Generate(1);
            var updatedPerson = updatedPersons.Single();
            updatedPerson.Id = personId;
            var result = await PersonCollection.ReplaceOneAsync(
                Builders<MongoPerson>.Filter.Eq(x => x.Id, personId), updatedPerson, cancellationToken: cancellationToken);
            if (result.ModifiedCount != 1)
            {
                throw new InvalidOperationException("Failed to update person");
            }
            return updatedPerson;
        }

        public async Task<ICollection<MongoCompositeIdPerson>> AddNewCompositeIdPersonsAsync(int count = 10, CancellationToken cancellationToken = default)
        {
            var personsToAdd = MongoCompositeIdPerson.Generate(count);
            await CompositeIdPersonCollection.InsertManyAsync(personsToAdd, cancellationToken: cancellationToken);
            return personsToAdd;
        }

        public async Task<MongoCompositeIdPerson> UpdateCompositeIdPersonAsync(PersonCompositeId personId, CancellationToken cancellationToken = default)
        {
            var updatedPersons = MongoCompositeIdPerson.Generate(1);
            var updatedPerson = updatedPersons.Single();
            updatedPerson.Id = personId;
            var result = await CompositeIdPersonCollection.ReplaceOneAsync(
                Builders<MongoCompositeIdPerson>.Filter.Eq(x => x.Id, personId), updatedPerson, cancellationToken: cancellationToken);
            if (result.ModifiedCount != 1)
            {
                throw new InvalidOperationException("Failed to update person");
            }
            return updatedPerson;
        }

        public async Task DeletePersonAsync(Guid personId, CancellationToken cancellationToken = default)
        {
            var result = await PersonCollection.DeleteOneAsync(Builders<MongoPerson>.Filter.Eq(x => x.Id, personId), cancellationToken);
            if (result.DeletedCount != 1)
            {
                throw new InvalidOperationException("Failed to delete person");
            }
        }

        public async Task DeleteCompositeIdPersonAsync(PersonCompositeId personId, CancellationToken cancellationToken = default)
        {
            var result = await CompositeIdPersonCollection.DeleteOneAsync(Builders<MongoCompositeIdPerson>.Filter.Eq(x => x.Id, personId), cancellationToken);
            if (result.DeletedCount != 1)
            {
                throw new InvalidOperationException("Failed to delete person");
            }
        }

        public Task DeleteAllPersonsAsync(CancellationToken cancellationToken = default)
        {
            return PersonCollection.DeleteManyAsync(Builders<MongoPerson>.Filter.Empty, cancellationToken);
        }

        public Task DeleteAllCompositeIdPersonsAsync(CancellationToken cancellationToken = default)
        {
            return CompositeIdPersonCollection.DeleteManyAsync(Builders<MongoCompositeIdPerson>.Filter.Empty, cancellationToken);
        }

        public Task ResetReplicationStateAsync(string replicationId, CancellationToken cancellationToken = default)
        {
            return ReplicationStateCollection.ReplaceOneAsync(
                filter: Builders<StateMongoDbDocument>.Filter.Eq(x => x.Id, replicationId),
                replacement: new StateMongoDbDocument { Id = replicationId },
                options: new ReplaceOptions { IsUpsert = true },
                cancellationToken: cancellationToken);
        }

        public async Task<StateMongoDbDocument?> GetReplicationStateOrDefaultAsync(string replicationId, CancellationToken cancellationToken = default)
        {
            var cursor = await ReplicationStateCollection.FindAsync(
                filter: Builders<StateMongoDbDocument>.Filter.Eq(x => x.Id, replicationId),
                options: default,
                cancellationToken: cancellationToken);

            return await cursor.SingleOrDefaultAsync(cancellationToken);
        }

        public void Dispose()
        {
            _serviceProvider.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}

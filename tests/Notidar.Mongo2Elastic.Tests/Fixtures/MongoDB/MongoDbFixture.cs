using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using Notidar.Mongo2Elastic.Tests.Fixtures.MongoDB.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Notidar.Mongo2Elastic.Tests.Fixtures.MongoDB
{
    public class MongoDbFixture : IDisposable
    {
        private readonly ServiceProvider _serviceProvider;

        public IMongoClient Client { get; private set; }
        public IMongoDatabase Database { get; private set; }
        public IMongoCollection<Person> PersonCollection { get; private set; }
        public IMongoCollection<ReplicationState> ReplicationStateCollection { get; private set; }

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
            PersonCollection = Database.GetCollection<Person>("persons");
            ReplicationStateCollection = Database.GetCollection<ReplicationState>("replications");
        }

        public async Task<ICollection<Person>> AddNewPersonsAsync(int count = 10, CancellationToken cancellationToken = default)
        {
            var personsToAdd = Person.Generate(count);
            await PersonCollection.InsertManyAsync(personsToAdd, cancellationToken: cancellationToken);
            return personsToAdd;
        }

        public async Task DeletePersonAsync(Guid personId, CancellationToken cancellationToken = default)
        {
            var result = await PersonCollection.DeleteOneAsync(Builders<Person>.Filter.Eq(x => x.Id, personId), cancellationToken);
            if (result.DeletedCount != 1)
            {
                throw new InvalidOperationException("Failed to delete person");
            }
        }

        public Task DeleteAllPersonsAsync(CancellationToken cancellationToken = default)
        {
            return PersonCollection.DeleteManyAsync(Builders<Person>.Filter.Empty, cancellationToken);
        }

        public Task ResetReplicationStateAsync(string replicationId, CancellationToken cancellationToken = default)
        {
            return ReplicationStateCollection.ReplaceOneAsync(
                filter: Builders<ReplicationState>.Filter.Eq(x => x.Id, replicationId),
                replacement: new ReplicationState { Id = replicationId },
                options: new ReplaceOptions { IsUpsert = true },
                cancellationToken: cancellationToken);
        }

        public async Task<ReplicationState?> GetReplicationStateOrDefaultAsync(string replicationId, CancellationToken cancellationToken = default)
        {
            var cursor = await ReplicationStateCollection.FindAsync(
                filter: Builders<ReplicationState>.Filter.Eq(x => x.Id, replicationId),
                options: default,
                cancellationToken: cancellationToken);

            return await cursor.SingleOrDefaultAsync();
        }

        public void Dispose()
        {
            _serviceProvider.Dispose();
        }
    }
}

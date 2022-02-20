using MongoDB.Bson.Serialization;
using System.Runtime.CompilerServices;

namespace Notidar.Mongo2Elastic.Tests.Fixtures.MongoDB.Models
{
    public static class MongoDbExtensions
    {
        [ModuleInitializer]
        public static void AddMongoDbMappings()
        {
            BsonClassMap.RegisterClassMap<CompositeIdPerson>(t =>
            {
                t.AutoMap();
                t.MapIdField(c => c.Id);
            });

            BsonClassMap.RegisterClassMap<Person>(t =>
            {
                t.AutoMap();
                t.MapIdField(c => c.Id);
            });
        }
    }
}
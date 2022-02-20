using Notidar.Mongo2Elastic.Builder;
using Notidar.Mongo2Elastic.Elasticsearch;

namespace Notidar.Mongo2Elastic.Tests.Fixtures.Elasticsearch.Models
{
    public class ElasticPerson
    {
        public string Id { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string UserName { get; set; }
        public string Email { get; set; }

        public static ElasticPerson FromMongoPerson(MongoDB.Models.MongoPerson person)
        {
            return new ElasticPerson
            {
                Id = person.Id.ToString(),
                Email = person.Email,
                FirstName = person.FirstName,
                LastName = person.LastName,
                UserName = person.UserName,
            };
        }

        public static ElasticPerson FromMongoCompositeIdPerson(MongoDB.Models.CompositeIdPerson person)
        {
            return new ElasticPerson
            {
                Id = person.Id.ToString(),
                Email = person.Email,
                FirstName = person.FirstName,
                LastName = person.LastName,
                UserName = person.UserName,
            };
        }
    }
}

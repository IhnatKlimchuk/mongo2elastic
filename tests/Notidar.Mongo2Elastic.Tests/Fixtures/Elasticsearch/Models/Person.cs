namespace Notidar.Mongo2Elastic.Tests.Fixtures.Elasticsearch.Models
{
    public class Person
    {
        public string Id { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string UserName { get; set; }
        public string Email { get; set; }

        public static Person FromMongoPerson(MongoDB.Models.Person person)
        {
            return new Person
            {
                Id = person.Id.ToString(),
                Email = person.Email,
                FirstName = person.FirstName,
                LastName = person.LastName,
                UserName = person.UserName,
            };
        }

        public static Person FromMongoCompositeIdPerson(MongoDB.Models.CompositeIdPerson person)
        {
            return new Person
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

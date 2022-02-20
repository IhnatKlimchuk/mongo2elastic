using Bogus;
using System;
using System.Collections.Generic;

namespace Notidar.Mongo2Elastic.Tests.Fixtures.MongoDB.Models
{
    public class CompositeIdPerson
    {
        public PersonCompositeId Id { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string UserName { get; set; }
        public string Email { get; set; }

        public static ICollection<CompositeIdPerson> Generate(int count = 10)
        {
            var faker = new Faker<CompositeIdPerson>()
                .RuleFor(x => x.Id, f => new PersonCompositeId { Id = Guid.NewGuid(), CountryCode = f.Address.CountryCode() })
                .RuleFor(u => u.FirstName, (f, u) => f.Name.FirstName())
                .RuleFor(u => u.LastName, (f, u) => f.Name.LastName())
                .RuleFor(u => u.UserName, (f, u) => f.Internet.UserName(u.FirstName, u.LastName))
                .RuleFor(u => u.Email, (f, u) => f.Internet.Email(u.FirstName, u.LastName));

            return faker.Generate(count);
        }

        public class PersonCompositeId
        {
            public Guid Id { get; set; }
            public string CountryCode { get; set; }
        }
    }
}

using Bogus;
using System;
using System.Collections.Generic;

namespace Notidar.Mongo2Elastic.Tests.Fixtures.MongoDB.Models
{
    public class Person
    {
        public Guid Id { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string UserName { get; set; }
        public string Email { get; set; }

        public static ICollection<Person> Generate(int count = 10)
        {
            var faker = new Faker<Person>()
                .RuleFor(x => x.Id, f => Guid.NewGuid())
                .RuleFor(u => u.FirstName, (f, u) => f.Name.FirstName())
                .RuleFor(u => u.LastName, (f, u) => f.Name.LastName())
                .RuleFor(u => u.UserName, (f, u) => f.Internet.UserName(u.FirstName, u.LastName))
                .RuleFor(u => u.Email, (f, u) => f.Internet.Email(u.FirstName, u.LastName));

            return faker.Generate(count);
        }
    }
}

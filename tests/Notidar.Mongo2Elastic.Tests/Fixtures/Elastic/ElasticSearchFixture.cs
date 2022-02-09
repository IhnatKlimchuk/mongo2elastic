using Elasticsearch.Net;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Nest;
using System;
using System.IO;

namespace Notidar.Mongo2Elastic.Tests.Fixtures.Elastic
{
    public class ElasticSearchFixture : IDisposable
    {
        private readonly ServiceProvider _serviceProvider;

        public IElasticClient Client { get; private set; }

        public ElasticSearchFixture()
        {
            var configuration = new ConfigurationBuilder()
                  .SetBasePath(Directory.GetCurrentDirectory())
                  .AddJsonFile("appsettings.json", optional: false)
                  .Build();

            _serviceProvider = new ServiceCollection()
                .Configure<DestinationOptions>(configuration.GetSection(nameof(DestinationOptions)))
                .BuildServiceProvider();

            var destinationOptions = _serviceProvider.GetRequiredService<IOptions<DestinationOptions>>();

            var url = new Uri(destinationOptions.Value.ElasticSearchUrl);
            var pool = new SingleNodeConnectionPool(url);
            var config = new ConnectionSettings(pool)
                .DefaultMappingFor<Person>(p => p
                .IndexName("persons")
                .IdProperty(x => x.Id))
                .EnableDebugMode()
                .PrettyJson()
                .MaximumRetries(3)
                .RequestTimeout(TimeSpan.FromSeconds(30))
                .MaxRetryTimeout(TimeSpan.FromSeconds(60));
            Client = new ElasticClient(config);
        }

        public void Dispose()
        {
            _serviceProvider.Dispose();
        }
    }
}

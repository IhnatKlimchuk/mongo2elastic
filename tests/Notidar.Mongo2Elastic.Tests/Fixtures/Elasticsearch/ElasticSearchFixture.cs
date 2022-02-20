using Elasticsearch.Net;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Nest;
using Notidar.Mongo2Elastic.Tests.Fixtures.Elasticsearch.Models;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Notidar.Mongo2Elastic.Tests.Fixtures.Elasticsearch
{
    public class ElasticsearchFixture : IDisposable
    {
        private readonly ServiceProvider _serviceProvider;

        public IElasticClient Client { get; private set; }

        public ElasticsearchFixture()
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
                .DefaultMappingFor<ElasticPerson>(p => p
                    .IndexName("persons")
                    .IdProperty(x => x.Id))
                .EnableDebugMode()
                .PrettyJson()
                .MaximumRetries(3)
                .RequestTimeout(TimeSpan.FromSeconds(30))
                .MaxRetryTimeout(TimeSpan.FromSeconds(60));
            Client = new ElasticClient(config);
        }

        public async Task DeleteAllPersonsAsync(CancellationToken cancellationToken = default)
        {
            var response = await Client.DeleteByQueryAsync<ElasticPerson>(del => del
                .Conflicts(Conflicts.Proceed)
                .Query(q => q.QueryString(qs => qs.Query("*"))), cancellationToken);
            if (!response.IsValid)
            {
                throw new InvalidOperationException("Failed to delete all persons from index.");
            }
        }

        public async Task<ElasticPerson> GetPersonOrDefaultAsync(string personId, CancellationToken cancellationToken = default)
        {
            var response = await Client.GetAsync<ElasticPerson>(personId, ct: cancellationToken);
            if (!response.IsValid && response.ApiCall.HttpStatusCode != 404)
            {
                throw new InvalidOperationException("Failed to get person by id", response.OriginalException);
            }

            return response.Source;
        }

        public async Task<long> CountPersonsAsync(CancellationToken cancellationToken = default)
        {
            var response = await Client.CountAsync<ElasticPerson>(ct: cancellationToken);
            if (!response.IsValid)
            {
                throw new InvalidOperationException("Failed to count persons", response.OriginalException);
            }

            return response.Count;
        }

        public void Dispose()
        {
            _serviceProvider.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}

using MongoDB.Bson;
using MongoDB.Driver;

namespace Notidar.Mongo2Elastic.MongoDB
{
    public class SourceRepository<TDocument> : ISourceRepository<TDocument> where TDocument : class
    {
        private readonly IMongoCollection<TDocument> _documentCollection;
        private readonly SourceRepositoryOptions<TDocument> _options;
        private readonly ProjectionDefinition<TDocument, TDocument> _projection;
        public SourceRepository(
            IMongoCollection<TDocument> documentCollection,
            SourceRepositoryOptions<TDocument> options)
        {
            _documentCollection = documentCollection;
            _options = options;

            if (options.ProjectionExcludeFields != null && options.ProjectionExcludeFields.Count > 0)
            {
                ProjectionDefinition<TDocument> projection = new BsonDocumentProjectionDefinition<TDocument>(new BsonDocument());
                foreach (var projectionExcludeField in options.ProjectionExcludeFields)
                {
                    projection = projection.Exclude(projectionExcludeField);
                }
                _projection = projection;
            }
        }

        public async Task<IAsyncEnumerable<IEnumerable<TDocument>>> GetDocumentsAsync(CancellationToken cancellationToken = default)
        {
            var cursor = await _documentCollection.FindAsync(
                filter: Builders<TDocument>.Filter.Empty,
                options: new FindOptions<TDocument>
                {
                    Projection = _projection,
                    BatchSize = _options.BatchSize,
                },
                cancellationToken: cancellationToken);

            return cursor.ToAsyncEnumerable(cancellationToken);
        }

        public async Task<IAsyncReplicationStream<TDocument>?> TryGetStreamAsync(string? resumeToken = null, CancellationToken cancellationToken = default)
        {
            try
            {
                var pipelineStages = new List<IPipelineStageDefinition>
                {
                    PipelineStageDefinitionBuilder.Project<ChangeStreamDocument<TDocument>, ChangeStreamDocument<TDocument>>(new BsonDocument
                    {
                        { "ns", 0},
                        { "to", 0},
                        { "updateDescription", 0 },
                        { "clusterTime", 0},
                        { "txnNumber", 0},
                        { "lsid", 0},
                    })
                };

                if (_projection != null)
                {
                    pipelineStages.Add(PipelineStageDefinitionBuilder.Project<ChangeStreamDocument<TDocument>, ChangeStreamDocument<TDocument>>(new BsonDocument
                    {
                        { "fullDocument", _projection.Render(_documentCollection.DocumentSerializer, _documentCollection.Settings.SerializerRegistry).Document},
                    }));
                }

                var streamCursor = await _documentCollection.WatchAsync<ChangeStreamDocument<TDocument>>(
                    pipeline: pipelineStages,
                    options: new ChangeStreamOptions
                    {
                        MaxAwaitTime = _options.MaxAwaitTime,
                        FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                        StartAfter = resumeToken == null ? null : new BsonDocument("_data", resumeToken),
                        BatchSize = _options.BatchSize
                    },
                    cancellationToken: cancellationToken);

                return new MongoAsyncReplicationStream<TDocument>(streamCursor);
            }
            catch
            {
                return null;
            }
        }
    }
}

# Mongo2Elastic

Strongly typed MongoDB to Elasticsearch replication library.

Supports:
- Replication conversion
- AddOrUpdate, Resetable and Versioned replication

### Roadmap

- [x] Create PoC
- [x] Add on the fly conversion
- [ ] Add MongoDB projection pipeline support
- [ ] Finalizing abstractions
- [ ] Finalizing package structure: Elasticsearch and MongoDb package naming for different versions: 7.x and 6.x
- [ ] Finalizing fluent builder
- [ ] Setup gihub actions CI pipeline
- [ ] Complete unit tests
- [ ] Complete integration tests
- [ ] Setup Nuget packages publishing
- [ ] Write wiki/documentation
- [ ] Create sample projects

### Areas for invetigation

- Ability to support delta updates from MongoDB change streams

### Integration tests

Run `docker-compose -f docker-compose.tests.yml -p m2e-test up` to start MongoDB and Elastisearch locally.

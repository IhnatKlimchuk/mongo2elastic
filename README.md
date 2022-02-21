# Mongo2Elastic
=======

Strongly typed MongoDB to Elasticsearch replication library.

Supports:
- Replication converting
- AddOrUpdate, Resetable and Versioned replication

### Roadmap

- [x] Create PoC
- [ ] Add MongoDB projection pipeline support
- [ ] Finalizing abstractions
- [ ] Finalizing package structure: Elasticsearch and MongoDb package naming for different versions: 7.x and 6.x
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
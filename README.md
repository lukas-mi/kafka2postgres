# kafka2postgres
POC service aggregating and loading user events (create, update, delete, forget) from Kafka to Postgres DB with custom business logic.

## Prerequisites:
- [sbt](https://www.scala-sbt.org/) for building Scala code
- [docker](https://docs.docker.com/engine/install/) for integration testing
- `sbt avroGenerate` to generate Java sources for [AVRO event schemas](src/main/avro)

## Run application
- `sbt run`

## Run integration tests
- `sbt test`

## Schemas
- [AVRO](src/main/avro)
- [Postgres](src/main/sql)

## Implementation details
- **kafka2postgres** consumes events from Kafka and performs CRUD operations to Postgres tables `users` and `users_forgotten`
  - Consumer offsets are committed manually once all events are applied to the DB
  - `UserForgotten` event handling involves two table `users` and `users_forgotten` where in a SQL transactions a record from `users` is deleted while a record to `users_forgotten` is inserted
- **kafka2postgres** uses headers in each Kafka message to decide on which event schema to use for decoding the record in a type safe way

## Integration tests
- Integration tests starts Kafka and Postgres containers using [testcontainers](https://testcontainers.com/)

## TODO:
- Add more tests covering specific business cases
- Add tests that disrupt network, stop Kafka/Postgres
- Add monitoring
- Package to Docker image
- Implement retry logic
- Use connection pooling when connecting to Postgres

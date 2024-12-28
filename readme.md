# hazelcast-kafka-avro

This project contains an experiment on integrating kafka with hazelcast.

Kafka is a great technology for event streaming and processing. As data is stored in a log format - data can be
efficiently replayed to recompute any processing.
Hazelcast is a distributed in-memory data platform. It is great for low latency data storage and processing.

Even though replaying kafka topics are fairly performant, an application may replay a complete kafka topic on startup to
recompute/process each message to work out the current state of the world.
This process may take a while and cause slow start up times.

In this experiment want to test a minimal example of how to cache avro data from kafka in hazelcast.

There are three applications in this project:

- **writer**: writes data in avro format and publishes data to a kafka broker.
- **pipeline**: consumes the data stored in avro format from kafka and writes the data as bytes in a hazelcast map
- **reader**: reads and deserialises the data from hazelcast

There would be a few benefits of externalising the result of data processing in hazelcast.
Separating data from application logic would make an application stateless.

It would create an easier release process and make it easier to horizontally scale.

The **pipeline** application submits code that remotely executes on hazelcast itself to keep the cache updated.
Hazelcast can be deployed as a cluster, so this adds extra redundancy to both the running pipeline code and the cache
that it is populating. Furthermore, the data processing and caching can continue even if the pipeline code goes down. 

However, as hazelcast stores and processes data in memory - kafka can be used to rehydrate the cache if needed in a
scenario where hazelcast was to completely go down for some reason.


## Running this project

### Dependencies

- Java 21
- Maven >= 3.9.6
- Docker and docker compose

### Starting

#### Compile sources

```shell
mvn clean install
```

#### Run a kafka broker and hazelcast instance

```shell
docker-compose up -d
```

#### Run writer

```shell
java -jar ./writer/target/writer.jar
```

#### Run pipeline

```shell
java -jar ./writer/target/pipeline.jar
```

#### Run reader

```shell
java -jar ./writer/target/reader.jar
```


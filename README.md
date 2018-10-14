# YugaByte DB sample apps

This repository emulates various workloads against YugaByte DB. YugaByte DB is a multi-model database that supports:
* PostgreSQL (sharded, scale-out SQL)
* YCQL (Cassandra compatible API with indexes, transactions and the JSONB data type)
* YEDIS (Redis compatible API with elasticity and persistence).

The sample apps here have drivers compatible with the above and emulate workloads such as:
* Key value inserts and lookup
* Batched inserts and lookups
* Transactional inserts
* Inserts and lookups with secondary index enabled

## Building the apps

You need the following to build:
* Java 1.8 or above
* Maven version 3.3.9 or above

To build, simply run the following:
```
$ mvn -DskipTests package
```

You can find the executable one-jar at the following location:
```
$ ls target/yb-sample-apps.jar
target/yb-sample-apps.jar
```

## Running the apps

For help, simply run the following:

```
$ java -jar target/yb-sample-apps.jar --help --nodes 127.0.0.1 --workload CassandraKeyValue
```

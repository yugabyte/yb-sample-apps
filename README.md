# YugaByte DB sample apps

This repository emulates various workloads against YugaByte DB. YugaByte DB is a multi-model database that supports:
* PostgreSQL (sharded, scale-out SQL)
* YCQL (Cassandra compatible API with indexes, transactions and the JSONB data type)
* YEDIS (Redis compatible API with elasticity and persistence).

The sample apps here have drivers compatible with the above and emulate a number of workloads.

| App Name                         | Description      |
| -------------------------------- | ---------------- |
| CassandraHelloWorld              | A very simple app that writes and reads one employee record into an 'Employee' table |
| CassandraKeyValue                | Sample key-value app built on Cassandra with concurrent reader and writer threads. |
| CassandraBatchKeyValue           | Sample batch key-value app built on Cassandra with concurrent reader and writer threads.|
| CassandraBatchTimeseries         | Timeseries/IoT app built that simulates metric data emitted by devices periodically.|
| CassandraTransactionalKeyValue   | Key-value app with multi-row transactions. Each write txn inserts a pair of unique string keys with the same value. |
| CassandraTransactionalRestartRead| This workload writes one key per thread, each time incrementing it's value and storing it in array. |
| CassandraStockTicker             | Sample stock ticker app built on CQL. Models stock tickers each of which emits quote data every second. |
| CassandraTimeseries              | Sample timeseries/IoT app built on CQL. The app models users with devices, each emitting multiple metrics per second. |
| CassandraUserId                  | Sample user id app built on Cassandra. The app writes out 1M unique user ids |
| CassandraPersonalization         | User personalization app. Writes unique customer ids, each with a set of coupons for different stores. |
| CassandraSecondaryIndex          | Secondary index on key-value YCQL table. Writes unique keys with an index on values. Query keys by values|
| CassandraUniqueSecondaryIndex    | Sample key-value app built on Cassandra. The app writes out unique string keys |
| RedisKeyValue                    | Sample key-value app built on Redis. The app writes out unique string keys each with a string value. |
| RedisPipelinedKeyValue           | Sample batched key-value app built on Redis. The app reads and writes a batch of key-value pairs. |
| RedisHashPipelined               | Sample redis hash-map based app built on RedisPipelined for batched operations. |
| RedisYBClientKeyValue            | Sample key-value app built on Redis that uses the YBJedis (multi-node) client instead|
| SqlInserts                       | Sample key-value app built on PostgreSQL with concurrent readers and writers. The app inserts unique string keys |
| SqlUpdates                       | Sample key-value app built on PostgreSQL with concurrent readers and writers. The app updates existing string keys |
| SqlSecondaryIndex                | Sample key-value app built on postgresql. The app writes out unique string keys |
| SqlSnapshotTxns                  | Sample key-value app built on postgresql. The app writes out unique string keys |

## Running the apps

For help, simply run the following:

```
$ java -jar target/yb-sample-apps.jar --help
```
You should see the set of workloads available in the app.

To get details on running any app, just pass the app name as a parameter to the `--help` flag:
```
$ java -jar target/yb-sample-apps.jar --help CassandraKeyValue
1 [main] INFO com.yugabyte.sample.Main  - Starting sample app...
Usage and options for workload CassandraKeyValue in YugaByte DB Sample Apps.

 - CassandraKeyValue :
   -----------------
		Sample key-value app built on Cassandra with concurrent reader and writer threads.
		 Each of these threads operates on a single key-value pair. The number of readers
		 and writers, the value size, the number of inserts vs updates are configurable.

		Usage:
			java -jar yb-sample-apps.jar \
			--workload CassandraKeyValue \
			--nodes 127.0.0.1:9042

		Other options (with default values):
			[ --num_unique_keys 1000000 ]
			[ --num_reads -1 ]
			[ --num_writes -1 ]
			[ --value_size 0 ]
			[ --num_threads_read 24 ]
			[ --num_threads_write 2 ]
			[ --table_ttl_seconds -1 ]
```

## Building the apps

You need the following to build:
* Java 1.8 or above
* Maven version 3.3.9 or above

To build, simply run the following:
```
$ mvn -DskipTests -DskipDockerBuild package
```

You can find the executable one-jar at the following location:
```
$ ls target/yb-sample-apps.jar
target/yb-sample-apps.jar
```

To docker image with the package, simply run the following:
```
$ mvn package
```

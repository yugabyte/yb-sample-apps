# YugabyteDB workload generator

This repository emulates various workloads against YugabyteDB. YugabyteDB is a multi-model database that supports:
* YSQL (Distributed SQL API with joins. Compatible with PostgreSQL)
* YCQL (Flexible-schema API with indexes, transactions and the JSONB data type. Roots in Cassandra QL)
* YEDIS (Transactional KV API with elasticity and persistence. Compatible with Redis)

The workloads here have drivers compatible with the above and emulate a number of real-work scenarios.

## Running the generator

Download the [latest yb-sample-apps](https://github.com/yugabyte/yb-sample-apps/releases/latest) JAR. The command below downloads version 1.4.1.
```console
$ wget https://github.com/yugabyte/yb-sample-apps/releases/download/v1.4.1/yb-sample-apps.jar
```

For help, simply run the following:

```console
$ java -jar yb-sample-apps.jar --help
```
You should see the set of workloads available in the app.

To get details on running any app, just pass the app name as a parameter to the `--help` flag:
```console
$ java -jar yb-sample-apps.jar --help CassandraKeyValue
1 [main] INFO com.yugabyte.sample.Main  - Starting sample app...
Usage and options for workload CassandraKeyValue in YugabyteDB Sample Apps.

 - CassandraKeyValue :
   -----------------
		Sample key-value app built on Cassandra with concurrent reader and writer threads.
		 Each of these threads operates on a single key-value pair. The number of readers
		 and writers, the value size, the number of inserts vs updates are configurable.
                 By default number of reads and writes operations are configured to 1500000 and 2000000 respectively.
		 User can run read/write(both) operations indefinitely by passing -1 to --num_reads or --num_writes or both

		Usage:
			java -jar yb-sample-apps.jar \
			--workload CassandraKeyValue \
			--nodes 127.0.0.1:9042

		Other options (with default values):
			[ --num_unique_keys 2000000 ]
			[ --num_reads 1500000 ]
			[ --num_writes 2000000 ]
			[ --value_size 0 ]
			[ --num_threads_read 24 ]
			[ --num_threads_write 2 ]
			[ --table_ttl_seconds -1 ]
```

## Building the generator

You need the following to build:
* Java 1.8 or above
* Maven version 3.3.9 or above

To build, simply run the following:
```console
$ mvn -DskipTests -DskipDockerBuild package
```

You can find the executable one-jar at the following location:
```console
$ ls target/yb-sample-apps.jar
target/yb-sample-apps.jar
```

To docker image with the package, simply run the following:
```console
$ mvn package
```

## Supported Workloads

Below is a list of workloads.

| App Name                         | Description      |
| -------------------------------- | ---------------- |
| CassandraHelloWorld              | A very simple app that writes and reads one employee record into an 'Employee' table |
| CassandraInserts                 | Secondary index on key-value YCQL table. Writes unique keys with an index on values.|
| CassandraKeyValue                | Sample key-value app built on Cassandra with concurrent reader and writer threads. |
| CassandraRangeKeyValue           | Sample key-value app built on Cassandra. The app writes out unique keys, each has one hash and three range string parts.|
| CassandraBatchKeyValue           | Sample batch key-value app built on Cassandra with concurrent reader and writer threads.|
| CassandraBatchTimeseries         | Timeseries/IoT app built that simulates metric data emitted by devices periodically.|
| CassandraEventData               | A sample IoT event data application with batch processing. |
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
| SqlGeoPartitionedTable           | Sample app based on SqlInserts but uses a geo-partitioned table |
| SqlStaleReadDetector             | Sample app that detects stale reads. Also, useful for benchmarking restart read requests. |

## Load balancing support in SQL workloads

New load balancing features are introduced in SQL workloads. The changes resulting from this new feature are visible in:

* pom.xml: contains both the upstream PostgreSQL JDBC driver dependency as well as Yugabyte's smart driver dependency.

* SQL* workloads: can be started with either the Yugabyte's smart driver or with the upstream PostgreSQL driver.

* Yugabyte's smart driver: is the default driver.

* Three new arguments are introduced in the SQL* workloads:

  * `load_balance`: It is true by default. When load_balance is `true` then YB's smart driver is used by the sample apps. So if you have a YB cluster created with a replication factor (rf) of 3 and the total number of connections ( it is equal to the sum of reader and writer threads ) will be evenly distributed across the 3 servers. If you explicitly set load-balance to `false` then the upstream PostgreSQL JDBC driver will be used and it will be same as the current state of the sample apps.

  * `topology_keys`: This property needs to be set only when load_balance is `true` and ignored when it is `false`. You can setup a cluster with different servers in different availability zones and then can configure the `yb-sample-apps` Sql* workloads to only create connections on servers which belong to a specific topology.

       Example topology:
       | Servers | Cloud provider | Region | Zone |
       | :------ | :------------- | :----- | :--- |
       | server 1 | aws | us-east | us-east-1a|
       | server 1 | aws | us-east | us-east-1b|
       | server 1 | aws | us-west | us-west-1a|

       If you want all your operations to go the `us-east` region but load-balanced on the servers which are there in `us-east` then you can specify that through the topology_keys config option like `--topology_keys=aws.us-east.us-east-1a,aws.us-east.us-east-1b`.

    * `Fallback option`: With `topology_keys`, you can also provide fallback options via preference value.
    Using this, one can tell the driver to connect/fallback to servers in other placements in case all the servers in primary placement are unavailable/down.

      The preference value, which is optional, can range from 1 (default) to 10 and is specified with the prefix `:`. Value 1 means it is the primary placement, 2 means it is first fallback, 3 means it's second fallback, and so on.

      The driver falls back to entire cluster even if no explicit fallback is specified in topology_keys and no servers are available in the given placements.

      Example of topology_keys value which specifies `aws.us-west.us-west-1a` as the first fallback:
      ```
      "aws.us-east.us-east-1a:1,aws.us-west.us-west-1a:2"
      ```
      The steps to demonstrate usage of fallback option is given at the end.
  * `debug_driver`: This property is set to debug the smart driver behaviour. It will be ignored if load-balance property is set to `false`.

   Following is the `usage` for SqlInserts workload example with the new added arguments using the `--help` flag:

   ```java
   java -jar target/yb-sample-apps.jar --help SqlInserts
   ```

   ```output
   0 [main] INFO com.yugabyte.sample.Main - Starting sample app...
   Usage and options for workload SqlInserts in YugabyteDB Sample Apps.

   SqlInserts :
   Sample key-value app built on PostgreSQL with concurrent readers and writers. The app inserts unique string keys each with a string value to a postgres table with an index on the value column.
   There are multiple readers and writers that update these keys and read them
   for a specified number of operations,default value for read ops is 1500000 and write ops is 2000000, with the readers query the keys by the associated values that are indexed. Note that the number of reads and writes to perform can be specified as a parameter, user can run read/write(both) operations indefinitely by passing -1 to --num_reads or --num_writes or both.

   Usage:
    java -jar yb-sample-apps.jar \
    --workload SqlInserts \
    --nodes 127.0.0.1:5433

   Other options (with default values):
    [ --num_unique_keys 2000000 ]
    [ --num_reads 1500000 ]
    [ --num_writes 2000000 ]
    [ --num_threads_read 2 ]
    [ --num_threads_write 2 ]
    [ --load_balance true ]
    [ --topology_keys null ]
    [ --debug_driver false ]
  ```

### Try SQL workload with fallback option

1. Start a 3-node cluster with separate placement for each tserver
    ```
    ./bin/yb-ctl start --rf 3 --placement_info "aws.us-east.us-east-1a,aws.us-east.us-east-1b,aws.us-west.us-west-1a"
    ```
2. Start a SQL workload with load_balance and topology_keys with preference value
    ```
    java -jar yb-sample-apps.jar --workload SqlInserts --nodes 127.0.0.1:5433 --load_balance true --topology_keys "aws.us-east.us-east-1a:1,aws.us-west.us-west-1a:2"
    ```
3. While the workload is running, you can verify from `http://127.0.0.1:13000/rpcz` that all the connections are made to tserver-1 since it is in the primary placement zone `aws.us-east.us-east-1a`
4. While the workload is still running, stop the tserver-1 specified in `--nodes` above
    ```
    ./bin/yb-ctl stop_node 1
    ```
5. You will see that the workload logs error messages like `FATAL: Could not reconnect to database` but subsequently continues its operations
6. It does so because the app requests for new connections and driver falls back to node in `aws.us-west.us-west-1a`
7. You can verify from `http://127.0.0.3:13000/rpcz` that the connections are now created on tserver-3
8. In step 2 above, if you specify the topology_keys without the preference value (fallback option) instead, as given below, and stop the first tserver while the workload is running, you will notice that the new connections are created across both the remaining tservers
    ```
    --topology_keys "aws.us-east.us-east-1a"
    ```

## Throttling IO Throughput

Throttle operation throughput of read and write threads using the `--read_rate` and `--write_rate` options respectively. To run the workload in lock step mode (where only one operation occurs at a time), use the option `--lock_step`.

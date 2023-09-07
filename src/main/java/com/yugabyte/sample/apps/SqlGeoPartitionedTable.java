// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//
package com.yugabyte.sample.apps;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import com.yugabyte.sample.common.SimpleLoadGenerator.Key;

/**
 * This workload writes and reads some random string keys from a postgresql table.
 */
public class SqlGeoPartitionedTable extends AppBase {
  private static final Logger LOG = Logger.getLogger(SqlGeoPartitionedTable.class);

  // Static initialization of this workload's config. These are good defaults for getting a decent
  // read dominated workload on a reasonably powered machine. Exact IOPS will of course vary
  // depending on the machine and what resources it has to spare.
  static {
    // Disable the read-write percentage.
    appConfig.readIOPSPercentage = -1;
    // Set the read and write threads to 2 each.
    appConfig.numReaderThreads = 2;
    appConfig.numWriterThreads = 2;
    // The number of keys to read.
    appConfig.numKeysToRead = -1;
    // The number of keys to write. This is the combined total number of inserts and updates.
    appConfig.numKeysToWrite = -1;
    // The number of unique keys to write. This determines the number of inserts (as opposed to
    // updates).
    appConfig.numUniqueKeysToWrite = NUM_UNIQUE_KEYS;
    // Replication factor to use for the tablespaces being created for the test.
    appConfig.replicationFactor = 1;
  }

  // The default table name to create and use for CRUD ops.
  private static final String DEFAULT_TABLE_NAME = "SqlGeoPartitionedTable";

  // The shared prepared select statement for fetching the data.
  private volatile PreparedStatement preparedSelect = null;

  // The shared prepared insert statement for inserting the data.
  private volatile PreparedStatement preparedInsert = null;

  // Lock for initializing prepared statement objects.
  private static final Object prepareInitLock = new Object();

  public SqlGeoPartitionedTable() {
    buffer = new byte[appConfig.valueSize];
  }

  /**
   * Drop the table created by this app.
   */
  @Override
  public void dropTable() throws Exception {
    try (Connection connection = getPostgresConnection()) {
      connection.createStatement().execute("DROP TABLE IF EXISTS " + getTableName());
      LOG.info(String.format("Dropped table: %s", getTableName()));
    }
  }

  @Override
  public void createTablesIfNeeded(TableOp tableOp) throws Exception {

    try (Connection connection = getPostgresConnection();
         Statement statement = connection.createStatement()) {

      // Drop the table if requested.
      if (tableOp.equals(TableOp.DropTable)) {
        statement.execute(String.format("DROP TABLE IF EXISTS %s", getTableName()));
        LOG.info("Dropping any table(s) left from previous runs if any");
      }

      // Create tablespaces if necessary.
      if (appConfig.placementPolicies != null) {
        appConfig.tablespaces = new String[appConfig.numPartitions];
        for (int i = 0; i < appConfig.numPartitions; i++) {
          // Placement policy is of the type cloud.region.zone.
          // Split it into individual entities.
          final String placement = appConfig.placementPolicies[i];
          final String[] entities = placement.split("\\.");

          if (entities.length != 3) {
            LOG.error(String.format("Invalid placement info %s. It should " +
                                    "be of the form cloud.region.zone", placement));
            System.exit(1);
          }

          String tablespaceName = "tablespace" + String.valueOf(i);

          statement.execute(" CREATE TABLESPACE " + tablespaceName +
            "  WITH (replica_placement=" +
            "'{\"num_replicas\":" + appConfig.replicationFactor +
            ", \"placement_blocks\":[" +
            "{\"cloud\":\"" + entities[0] + "\"," +
            "\"region\":\"" + entities[1] + "\"," +
            "\"zone\":\"" + entities[2] + "\"," +
            "\"min_num_replicas\":" + appConfig.replicationFactor + "}]}')");

          // Add to the set of tablespaces to be used by the test.
          appConfig.tablespaces[i] = tablespaceName;
        }
      }

      // TODO Creating the primary keys on each partition because of issue #6149.
      statement.execute(
          String.format("CREATE TABLE IF NOT EXISTS %s (region text, k text, v text) " +
                            "PARTITION BY LIST (region)", getTableName()));
      // Creating partitions.
      for (int i = 0; i < appConfig.numPartitions; i++) {
        statement.execute(
            String.format("CREATE TABLE IF NOT EXISTS %1$s_%2$d PARTITION OF %1$s (" +
                              "region, k, v, PRIMARY KEY((region, k) HASH))" +
                              " FOR VALUES IN ('region_%2$d') TABLESPACE %3$s",
                          getTableName(), i, appConfig.tablespaces[i]));
      }

      LOG.info(String.format("Created (if not exists) table: %s", getTableName()));

      if (tableOp.equals(TableOp.TruncateTable)) {
        for (int i = 0; i < appConfig.numPartitions; i++) {
          statement.execute(
              String.format("TRUNCATE TABLE %s_%d", getTableName(), i));
        }
        LOG.info(String.format("Truncated table: %s", getTableName()));
      }
    }
  }

  public String getTableName() {
    String tableName = appConfig.tableName != null ? appConfig.tableName : DEFAULT_TABLE_NAME;
    return tableName.toLowerCase();
  }

  public String getRegion(Long key_number) {
    return String.format("region_%d", key_number % appConfig.numPartitions);
  }

  private PreparedStatement getPreparedSelect() throws Exception {
    if (preparedSelect == null) {
      preparedSelect = getPostgresConnection().prepareStatement(
          String.format("SELECT k, v FROM %s WHERE region = ? AND k = ?",
                        getTableName()));
    }
    return preparedSelect;
  }

  @Override
  public long doRead() {
    Key key = getSimpleLoadGenerator().getKeyToRead();
    if (key == null) {
      // There are no keys to read yet.
      return 0;
    }

    try {
      PreparedStatement statement = getPreparedSelect();
      statement.setString(1, getRegion(key.asNumber()));
      statement.setString(2, key.asString());
      try (ResultSet rs = statement.executeQuery()) {
        if (!rs.next()) {
          LOG.error("Read key: " + key.asString() + " expected 1 row in result, got 0");
          return 0;
        }

        if (!key.asString().equals(rs.getString("k"))) {
          LOG.error("Read key: " + key.asString() + ", got " + rs.getString("k"));
        }
        LOG.debug("Read key: " + key.toString());

        key.verify(rs.getString("v"));

        if (rs.next()) {
          LOG.error("Read key: " + key.asString() + " expected 1 row in result, got more");
          return 0;
        }
      }
    } catch (Exception e) {
      LOG.info("Failed reading value: " + key.getValueStr(), e);
      preparedSelect = null;
      return 0;
    }
    return 1;
  }

  private PreparedStatement getPreparedInsert() throws Exception {
    if (preparedInsert == null) {
      Connection connection = getPostgresConnection();
      connection.createStatement().execute("set yb_enable_upsert_mode = true");
      preparedInsert = connection.prepareStatement(
          String.format("INSERT INTO %s (region, k, v) VALUES (?, ?, ?)",
                        getTableName()));
    }
    return preparedInsert;
  }

  @Override
  public long doWrite(int threadIdx) {
    Key key = getSimpleLoadGenerator().getKeyToWrite();
    if (key == null) {
      return 0;
    }

    int result = 0;
    try {
      PreparedStatement statement = getPreparedInsert();
      // Prefix hashcode to ensure generated keys are random and not sequential.
      statement.setString(1, getRegion(key.asNumber()));
      statement.setString(2, key.asString());
      statement.setString(3, key.getValueStr());
      result = statement.executeUpdate();
      LOG.debug("Wrote key: " + key.asString() + ", " + key.getValueStr() + ", return code: " +
                    result);
      getSimpleLoadGenerator().recordWriteSuccess(key);
    } catch (Exception e) {
      getSimpleLoadGenerator().recordWriteFailure(key);
      LOG.info("Failed writing key: " + key.asString(), e);
      preparedInsert = null;
    }
    return result;
  }

  @Override
  public List<String> getWorkloadDescription() {
    return Arrays.asList(
        "Sample app based on SqlInserts but uses a geo-partitioned table.",
        "It creates a list-partitioned table with an additional primary-key column ",
        "'region', and a configurable number of partitions.",
        "Each partition is placed in a separate tablespace. These tablespaces can be ",
        "pre-created and provided with the --tablespaces option or they can be ",
        "created within the app itself based on the --placement_policies option.",
        "The value(s) for the column (region) is automatically generated for each",
        "read/write operation based on the randomly-generated key column ('k').");
  }

  @Override
  public List<String> getWorkloadOptionalArguments() {
    return Arrays.asList(
        "--num_partitions " + appConfig.numPartitions,
        "--tablespaces " + appConfig.tablespaces,
        "--replication_factor " + appConfig.replicationFactor,
        "--placement_policies " + appConfig.placementPolicies,
        "--num_threads_read " + appConfig.numReaderThreads,
        "--num_threads_write " + appConfig.numWriterThreads,
        "--num_unique_keys " + appConfig.numUniqueKeysToWrite,
        "--num_reads " + appConfig.numKeysToRead,
        "--num_writes " + appConfig.numKeysToWrite,
        "--load_balance " + appConfig.loadBalance,
        "--topology_keys " + appConfig.topologyKeys,
        "--debug_driver " + appConfig.enableDriverDebug);
  }
}

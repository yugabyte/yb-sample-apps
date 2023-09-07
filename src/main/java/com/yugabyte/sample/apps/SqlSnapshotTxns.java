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
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import com.yugabyte.sample.apps.AppBase.TableOp;
import com.yugabyte.sample.common.SimpleLoadGenerator.Key;

import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;

/**
 * This workload writes and reads some random string keys from a postgresql table.
 */
public class SqlSnapshotTxns extends AppBase {
  private static final Logger LOG = Logger.getLogger(SqlSnapshotTxns.class);

  // Static initialization of this workload's config. These are good defaults for getting a decent
  // read dominated workload on a reasonably powered machine. Exact IOPS will of course vary
  // depending on the machine and what resources it has to spare.
  static {
    // Disable the read-write percentage.
    appConfig.readIOPSPercentage = -1;
    // Set the read and write threads to 1 each.
    appConfig.numReaderThreads = 1;
    appConfig.numWriterThreads = 1;
    // The number of keys to read.
    appConfig.numKeysToRead = -1;
    // The number of keys to write. This is the combined total number of inserts and updates.
    appConfig.numKeysToWrite = -1;
    // The number of unique keys to write. This determines the number of inserts (as opposed to
    // updates).
    appConfig.numUniqueKeysToWrite = NUM_UNIQUE_KEYS;
  }

  // The default table name to create and use for CRUD ops.
  private static final String DEFAULT_TABLE_NAME = "SqlSnapshotTxns";

  // The shared prepared select statement for fetching the data.
  private volatile PreparedStatement preparedSelect = null;

  // The shared prepared insert statement for inserting the data.
  private volatile PreparedStatement preparedInsert = null;

  // Lock for initializing prepared statement objects.
  private static final Object prepareInitLock = new Object();

  public SqlSnapshotTxns() {
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
    try (Connection connection = getPostgresConnection()) {
      connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      connection.setAutoCommit(false);
  
      // (Re)Create the table (every run should start cleanly with an empty table).
      // connection.createStatement().execute(
      //     String.format("DROP TABLE IF EXISTS %s", getTableName()));
      LOG.info("Dropping table(s) left from previous runs if any");
      connection.createStatement().executeUpdate(
          String.format("CREATE TABLE IF NOT EXISTS %s (k text PRIMARY KEY, v text);", getTableName()));
      LOG.info(String.format("Created table: %s", getTableName()));
    }
  }

  public String getTableName() {
    String tableName = appConfig.tableName != null ? appConfig.tableName : DEFAULT_TABLE_NAME;
    return tableName.toLowerCase();
  }

  private PreparedStatement getPreparedSelect() throws Exception {
    if (preparedSelect == null) {
      preparedSelect = getPostgresConnection().prepareStatement(
          String.format("SELECT k, v FROM %s WHERE k = ?;", getTableName()));
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
      statement.setString(1, key.asString());
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
    String stmt = "BEGIN TRANSACTION;" +
                  String.format("INSERT INTO %s (k, v) VALUES (?, ?);", getTableName()) +
                  String.format("INSERT INTO %s (k, v) VALUES (?, ?);", getTableName()) +
                  "COMMIT;";
    if (preparedInsert == null) {
      Connection connection = getPostgresConnection();
      connection.createStatement().execute("set yb_enable_upsert_mode = true");
      preparedInsert = connection.prepareStatement(stmt);
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
      statement.setString(1, key.asString());
      statement.setString(2, key.getValueStr());
      statement.setString(3, key.asString() + "-copy");
      statement.setString(4, key.getValueStr());
      result = statement.executeUpdate();
      LOG.debug("Wrote key: " + key.asString() + ", " + key.getValueStr() + ", return code: " +
                result);
      getSimpleLoadGenerator().recordWriteSuccess(key);
      return 1;
    } catch (Exception e) {
      getSimpleLoadGenerator().recordWriteFailure(key);
      LOG.info("Failed writing key: " + key.asString(), e);
      preparedInsert = null;
    }
    return 0;
  }

  @Override
  public List<String> getWorkloadDescription() {
    return Arrays.asList(
        "Sample key-value app built on postgresql. The app writes out unique string keys",
        "each with a string value to a postgres table with an index on the value column.",
        "There are multiple readers and writers that update these keys and read them",
        "indefinitely, with the readers query the keys by the associated values that are",
        "indexed. Note that the number of reads and writes to perform can be specified as",
        "a parameter.");
  }

  @Override
  public List<String> getWorkloadOptionalArguments() {
    return Arrays.asList(
        "--num_unique_keys " + appConfig.numUniqueKeysToWrite,
        "--num_reads " + appConfig.numKeysToRead,
        "--num_writes " + appConfig.numKeysToWrite,
        "--num_threads_read " + appConfig.numReaderThreads,
        "--num_threads_write " + appConfig.numWriterThreads,
        "--load_balance " + appConfig.loadBalance,
        "--topology_keys " + appConfig.topologyKeys,
        "--debug_driver " + appConfig.enableDriverDebug);
  }
}

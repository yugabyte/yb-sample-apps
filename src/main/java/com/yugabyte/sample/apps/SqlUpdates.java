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
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.yugabyte.sample.common.CmdLineOpts;
import org.apache.log4j.Logger;

import com.yugabyte.sample.common.SimpleLoadGenerator.Key;

/**
 * This workload writes and reads some random string keys from a postgresql table.
 */
public class SqlUpdates extends AppBase {
  private static final Logger LOG = Logger.getLogger(SqlUpdates.class);

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
  private static final String DEFAULT_TABLE_NAME = "PostgresqlKeyValue";

  // The shared prepared select statement for fetching the data.
  private volatile PreparedStatement preparedSelect = null;

  // The shared prepared insert statement for inserting the data.
  private volatile PreparedStatement preparedInsert = null;

  // Lock for initializing prepared statement objects.
  private static final Object prepareInitLock = new Object();

  public SqlUpdates() {
    buffer = new byte[appConfig.valueSize];
  }

  @Override
  public void createTablesIfNeeded() throws Exception {
    // Check that (extra_ required options are set (maxWrittenKey and loadTesterUUID) set.
    if (appConfig.maxWrittenKey <= 0) {
      LOG.fatal("Workload requires option --max_written_key to be set. \n " +
                    "Run 'java -jar yb-sample-apps.jar --help SqlUpdates' for more details");
      System.exit(1);
    }
    if (CmdLineOpts.loadTesterUUID == null) {
      LOG.fatal("Workload requires option --uuid to be set. \n " +
                    "Run 'java -jar yb-sample-apps.jar --help SqlUpdates' for more details");
      System.exit(1);
    }

    // Expecting that the SqlInserts ap was already run and created the table.
    // This app just reuses the table and updates the values for the existing rows.
    Connection connection = getPostgresConnection();
    DatabaseMetaData dbm = connection.getMetaData();
    ResultSet tables = dbm.getTables(null, null, getTableName(), null);
    if (!tables.next()) {
      LOG.fatal(
          String.format("Could not find the %s table. Did you run SqlInserts first?" +
                            "Run 'java -jar yb-sample-apps.jar --help SqlUpdates' for more details",
                        getTableName()));
      System.exit(1);
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

        if (rs.next()) {
          LOG.error("Read key: " + key.asString() + " expected 1 row in result, got more");
          return 0;
        }
      }
    } catch (Exception e) {
      LOG.fatal("Failed reading value: " + key.getValueStr(), e);
      return 0;
    }
    return 1;
  }

  private PreparedStatement getPreparedUpdate() throws Exception {
    if (preparedInsert == null) {
      preparedInsert = getPostgresConnection().prepareStatement(
          String.format("UPDATE %s SET v=? WHERE k=?;", getTableName()));
    }
    return preparedInsert;
  }

  @Override
  public long doWrite(int threadIdx) {
    // Get a key that has already been written (will need to implicitly read it to update).
    Key key = getSimpleLoadGenerator().getKeyToRead();
    if (key == null) {
      return 0;
    }

    int result = 0;
    try {
      PreparedStatement statement = getPreparedUpdate();
      // Prefix hashcode to ensure generated keys are random and not sequential.
      statement.setString(1, key.getValueStr() + ":" + System.currentTimeMillis());
      statement.setString(2, key.asString());
      result = statement.executeUpdate();
    } catch (Exception e) {
      LOG.fatal("Failed writing key: " + key.asString(), e);
    }
    return result;
  }

  @Override
  public List<String> getWorkloadDescription() {
    return Arrays.asList(
        "Sample key-value app built on PostgreSQL with concurrent readers and writers. ",
        "The app updates existing string keys loaded by a run of the SqlInserts sample app.",
        "There are multiple readers and writers that update these keys and read them",
        "indefinitely, with the readers query the keys by the associated values that are",
        "indexed. Note that the number of reads and writes to perform can be specified as",
        "a parameter.",
        "It requires two additional options: ",
        "   1. --max_written_key: set to the max key written by the SqlInserts app run.",
        "   2. --uuid: set to the uuid to the uuid prefix used by SqlInserts (recommended ",
        "     to (re)run SqlInserts with the '--uuid' option set, otherwise select from the ",
        "    " + getTableName() + " table to find out the used uuid.",
        "Example, run SqlInserts before as follows:",
        "java -jar yb-sample-apps.jar --workload SqlInserts --nodes 127.0.0.1:5433 \\",
        "                             --uuid 'ffffffff-ffff-ffff-ffff-ffffffffffff' \\",
        "                             --num_writes 100000 \\",
        "                             --num_reads 0"
        );
  }

  @Override
  public List<String> getWorkloadRequiredArguments() {
    return Arrays.asList(
        "--uuid 'ffffffff-ffff-ffff-ffff-ffffffffffff'",
        "--max_written_key 100000");
  }

  @Override
  public List<String> getWorkloadOptionalArguments() {
    return Arrays.asList(
        "--num_reads " + appConfig.numKeysToRead,
        "--num_writes " + appConfig.numKeysToWrite,
        "--num_threads_read " + appConfig.numReaderThreads,
        "--num_threads_write " + appConfig.numWriterThreads);
  }
}

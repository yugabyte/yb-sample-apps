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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.yugabyte.sample.common.CmdLineOpts;

/**
 * This workload sets up a bunch of counters initialized to zero.
 * 1. Write thread: Picks a random counter and then increments it.
 * 2. Read thread: Sums all the counters to verify that the read caught up to all the writes.
 */
public class SqlStaleReadDetector extends AppBase {
  private static final Logger LOG = Logger.getLogger(SqlStaleReadDetector.class);

  // Static initialization of this workload's config.
  static {
    // Drop the table and create a new one.
    appConfig.tableOp = TableOp.DropTable;
    // Disable the read-write percentage.
    appConfig.readIOPSPercentage = -1;
    // Increase writer threads to increase write throughput.
    appConfig.numReaderThreads = 1;
    appConfig.numWriterThreads = 10;
    // Disable num keys to read.
    appConfig.numKeysToRead = -1;
    // Disable num keys to write.
    appConfig.numKeysToWrite = -1;
    // Disable num unique keys to write.
    appConfig.numUniqueKeysToWrite = -1;
    // INSERT maxWrittenKey rows into the table before running the app.
    appConfig.maxWrittenKey = NUM_UNIQUE_KEYS;
    // Run the sum every 1 second.
    appConfig.maxReadThreadThroughput = 1;
    // Increment 100 counters each second.
    appConfig.maxWriteThreadThroughput = 100;
    // Run the app for 5 minutes.
    appConfig.runTimeSeconds = 300;
    // Do not use lock step by default.
    // Do not need lock step to detect stale reads but
    // stale reads can occur even with lock step.
    appConfig.concurrencyDisabled = false;
    // Report restart read requests metric by default.
    appConfig.restartReadsReported = true;
    // Disable YB load balancing to enforce round robin.
    appConfig.disableYBLoadBalancingPolicy = true;
  }

  // Should we sum the counters in the application instead of the database?
  private static boolean sumInApp = false;

  // The default table name to create and use for CRUD ops.
  private static final String DEFAULT_TABLE_NAME = "PostgresqlKeyValue";

  // The shared prepared select statement for fetching the data.
  private volatile PreparedStatement preparedSelect = null;

  // The shared prepared select statement for fetching the sum.
  private volatile PreparedStatement preparedSum = null;

  // The shared prepared update statement for updating the data.
  private volatile PreparedStatement preparedUpdate = null;

  // Store the number of times the counters were incremented to
  // detect stale reads.
  // This is a shared counter between the reader and writer threads.
  private static AtomicLong numIncrements = new AtomicLong(0);

  // Shared counter to store the number of stale reads.
  private static AtomicLong numStaleReads = new AtomicLong(0);

  public SqlStaleReadDetector() {}

  @Override
  public void initialize(CmdLineOpts configuration) {
    super.initialize(configuration);

    if (configuration.getCommandLine().hasOption("sum_in_app")) {
      sumInApp = true;
    }
  }

  /*
   * Create a key, value table, both integer data type.
   * Insert (a million by default) rows with their values initialized to zero.
   */
  @Override
  public void createTablesIfNeeded(TableOp tableOp) throws Exception {
    try (Connection connection = getPostgresConnection()) {
      // (Re)Create the table (every run should start cleanly with an empty table).
      if (tableOp.equals(TableOp.DropTable)) {
          connection.createStatement().execute(
              String.format("DROP TABLE IF EXISTS %s", getTableName()));
          LOG.info("Dropping any table(s) left from previous runs if any");
      }
      connection.createStatement().execute(String.format(
          "CREATE TABLE IF NOT EXISTS %s (k INT PRIMARY KEY, v INT) SPLIT INTO 24 TABLETS",
          getTableName()));
      LOG.info(String.format("Created table: %s", getTableName()));
      if (tableOp.equals(TableOp.TruncateTable)) {
      	connection.createStatement().execute(
            String.format("TRUNCATE TABLE %s", getTableName()));
        LOG.info(String.format("Truncated table: %s", getTableName()));
      }
      // INSERT 1 to maxWrittenKey, zero initialized.
      int numRows = connection.createStatement().executeUpdate(String.format(
          "INSERT INTO %s SELECT GENERATE_SERIES(1, %d), 0",
          getTableName(), appConfig.maxWrittenKey));
      LOG.info(String.format(
          "Inserted %d rows into %s", numRows, getTableName()));
    }
  }

  public String getTableName() {
    String tableName = appConfig.tableName != null ? appConfig.tableName : DEFAULT_TABLE_NAME;
    return tableName.toLowerCase();
  }

  private PreparedStatement getPreparedSelect() throws Exception {
    if (preparedSelect == null) {
      preparedSelect = getPostgresConnection().prepareStatement(
          String.format("SELECT v FROM %s;", getTableName()));
    }
    return preparedSelect;
  }

  private PreparedStatement getPreparedSum() throws Exception {
    if (preparedSum == null) {
      preparedSum = getPostgresConnection().prepareStatement(
          String.format("SELECT SUM(v) FROM %s;", getTableName()));
    }
    return preparedSum;
  }

  /*
   * Reads sum all values in the table.
   * Checks for stale reads by
   * 1. Reading the number of increments so far.
   * 2. Summing across all the values in the table.
   * The sum must be at least the number of increments.
   * Otherwise, the database definitely suffers from stale reads.
   *
   * One way to reproduce stale reads is:
   * 1. Limit hybrid time propagation.
   *   a. Use replication_factor=1
   *   b. Use a single zone.
   *   c. heartbeat_interval_ms=10000
   * 2. Disable clock skew related checks.
   *   T-Server GFlags
   *   a. max_clock_skew_usec=0
   *   b. fail_on_out_of_range_clock_skew=false
   *   c. clock_skew_force_crash_bound_usec=0
   * 3. Simulate clock skew.
   *   a. sudo systemctl stop chronyd
   *   b. sudo timedatectl set-ntp false
   *   c. sudo date -s "$(date -d '+0.4 seconds' '+%Y-%m-%d %H:%M:%S')"
   * As a final step, verify the clock skew between the cluster nodes
   * using the /tablet-server-clocks page of the WebUI.
   *
   * Records stale reads to be reported.
   */
  @Override
  public long doRead() {
    try {
      long previousCounter = numIncrements.get();
      PreparedStatement statement = sumInApp ? getPreparedSelect() : getPreparedSum();
      try (ResultSet rs = statement.executeQuery()) {
        if (!rs.next()) {
          LOG.error("Failed to read!", new IllegalStateException("No rows returned!"));
          return 0;
        }

        Long sum = 0L;
        if (sumInApp) {
          // Sum all the values returned.
          do {
            sum += rs.getLong(1);
          } while (rs.next());
        } else {
          // Database already returns the sum.
          sum = rs.getLong(1);
        }

        if (sum < previousCounter) {
          LOG.error(String.format(
            "Stale read detected! Expected previous counter = %d to be atmost sum = %d",
            previousCounter, sum));
          numStaleReads.incrementAndGet();
        }
      }
    } catch (Exception e) {
      LOG.error("Failed read!", e);
      preparedSelect = null;
      return 0;
    }
    return 1;
  }

  private PreparedStatement getPreparedUpdate() throws Exception {
    if (preparedUpdate == null) {
      preparedUpdate = getPostgresConnection().prepareStatement(
          String.format("UPDATE %s SET v=v+1 WHERE k=?;", getTableName()));
    }
    return preparedUpdate;
  }

  /*
   * Picks a random key and then increments it.
   * Additionally, increments the shared counter.
   */
  @Override
  public long doWrite(int threadIdx) {
    long key = getSimpleLoadGenerator().getKeyToRead().asNumber() + 1;

    int result = 0;
    try {
      PreparedStatement statement = getPreparedUpdate();
      statement.setLong(1, key);
      result = statement.executeUpdate();
      numIncrements.addAndGet(result);
    } catch (Exception e) {
      LOG.error(String.format("Failed incrementing key: %d", key), e);
      preparedUpdate = null;
    }
    return result;
  }

  /*
   * Appends the number of stale reads to the metrics output.
   */
  @Override
  public void appendMessage(StringBuilder sb) {
    sb.append("Stale reads: ").append(numStaleReads.get()).append(" total ops | ");
    super.appendMessage(sb);
  }

  @Override
  public List<String> getWorkloadDescription() {
    return Arrays.asList(
        "Sample key-value app built on PostgreSQL that detects stale reads.",
        " Initially inserts `maxWrittenKey` rows with value initialized to zero.",
        " The writer threads then increment the value of a random key.",
        " The reader threads concurrently sum up the values across all the keys.",
        " The reader threads also verify whether the sum is at least the number of increments.",
        " This check will help us detect any stale reads.",
        " Optionally use the maxWrittenKey config to set the number of rows to insert",
        " before running the app. Default: " + String.valueOf(NUM_UNIQUE_KEYS) + "."
        );
  }

  @Override
  public List<String> getWorkloadOptionalArguments() {
    return Arrays.asList(
        "--read_rate "  + appConfig.maxReadThreadThroughput,
        "--write_rate " + appConfig.maxWriteThreadThroughput,
        "--lock_step",
        "--max_written_key" + appConfig.maxWrittenKey,
        "--num_threads_read " + appConfig.numReaderThreads,
        "--num_threads_write " + appConfig.numWriterThreads,
        "--report_read_restarts",
        "--sum_in_app"
        );
  }
}

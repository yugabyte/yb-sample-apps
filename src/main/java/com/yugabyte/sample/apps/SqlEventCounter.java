// Copyright (c) YugabyteDB, Inc.
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
import java.sql.SQLException;

import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.log4j.Logger;

/*
 * Sometimes, applications want to track the number of times a particular event
 * has occurred. Examples include user actions like clicks, purchases or
 * page views.
 *
 * This app helps understand whether the new clockbound clock
 * helps improve the performance of this workload.
 *
 * Database Configuration:
 *  configure with wallclock and compare the metrics with
 *  a clockbound clock configuration.
 *  Not much variance is expected in the metrics.
 *
 * Setup:
 * 1. Create a counters TABLE with columns (event INT, counter INT).
 * 2. Insert 1000 counters with event 0 to 999 initialized to zero.
 *
 * Worklaod:
 * We only run write threads, no read threads.
 * Each write thread,
 * 1. Starts a repeatable read transaction.
 * 2. Reads the counter of a random event.
 * 3. Verifies that the counter is not stale.
 * 4. Increments the counter for the picked event.
 * 5. Commits the transaction.
 * 6. Updates the latest counter value in the cache.
 */
public class SqlEventCounter extends AppBase {
  private static final Logger LOG = Logger.getLogger(SqlEventCounter.class);

  // Static initialization of this app's config.
  static {
    // Only use 10 writer threads to avoid overloading the system.
    // In real life, there are many more threads but there are other
    // things to do too.
    appConfig.readIOPSPercentage = -1;
    appConfig.numReaderThreads = 0;
    appConfig.numWriterThreads = 10;
    // Disable number of keys.
    appConfig.numKeysToRead = -1;
    appConfig.numKeysToWrite = -1;
    // Run the app for 1 minute.
    appConfig.runTimeSeconds = 60;
    // Report restart read requests metric by default.
    appConfig.restartReadsReported = true;
    // Avoid load balancing errors.
    appConfig.loadBalance = false;
    appConfig.disableYBLoadBalancingPolicy = true;
  }

  // The default table name to create and use for ops.
  private static final String DEFAULT_TABLE_NAME = "event_counters";

  // The number of unique events to track.
  private static final int NUM_EVENTS = 1000;

  // Contains the latest updated counter indexed by event.
  private static final AtomicIntegerArray counters = new AtomicIntegerArray(NUM_EVENTS);

  // Shared counter to store the number of stale reads.
  private static final AtomicLong numStaleReads = new AtomicLong(0);

  // Cache connection and statements.
  private Connection connection = null;
  private PreparedStatement preparedCounterFetch = null;
  private PreparedStatement preparedCounterIncrement = null;

  public Connection getConnection() {
    if (connection == null) {
      try {
        connection = getPostgresConnection();
      } catch (Exception e) {
        LOG.fatal("Failed to create a connection ", e);
      }
    }
    return connection;
  }

  public PreparedStatement getPreparedCounterFetch() {
    if (preparedCounterFetch == null) {
      try {
        preparedCounterFetch = getConnection().prepareStatement(
            String.format("SELECT counter FROM %s WHERE event = ?",
                getTableName()));
      } catch (Exception e) {
        LOG.fatal("Failed to prepare statement: SELECT counter FROM " + getTableName(), e);
      }
    }
    return preparedCounterFetch;
  }

  public PreparedStatement getPreparedCounterIncrement() {
    if (preparedCounterIncrement == null) {
      try {
        preparedCounterIncrement = getConnection().prepareStatement(
            String.format("UPDATE %s SET counter = ? WHERE event = ?",
                getTableName()));
      } catch (Exception e) {
        LOG.fatal("Failed to prepare statement: UPDATE " + getTableName(), e);
      }
    }
    return preparedCounterIncrement;
  }

  @Override
  public void createTablesIfNeeded(TableOp tableOp) throws Exception {
    Connection connection = getConnection();
    // Every run should start cleanly.
    connection.createStatement().execute(
        String.format("DROP TABLE IF EXISTS %s", getTableName()));
    LOG.info("Dropping any table(s) left from previous runs if any");
    connection.createStatement().execute(String.format(
        "CREATE TABLE %s (event INT, counter INT)",
        getTableName()));
    LOG.info(String.format("Created table: %s", getTableName()));
    int numRows = connection.createStatement().executeUpdate(String.format(
        "INSERT INTO %s SELECT GENERATE_SERIES(0, %d-1), 0",
        getTableName(), NUM_EVENTS));
    LOG.info(String.format(
        "Inserted %d rows into %s", numRows, getTableName()));
  }

  @Override
  public String getTableName() {
    String tableName = appConfig.tableName != null ?
        appConfig.tableName : DEFAULT_TABLE_NAME;
    return tableName.toLowerCase();
  }

  @Override
  public long doWrite(int threadIdx) {
    // Choose a random event to increment.
    int event = ThreadLocalRandom.current().nextInt(NUM_EVENTS);
    Connection connection = getConnection();

    try {
      // Start a repeatable read transaction.
      connection.setAutoCommit(false);
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      // Retrieve the latest counter from the cache.
      int cachedCounter = counters.get(event);

      // Fetch the current counter value for the event.
      PreparedStatement preparedCounterFetch = getPreparedCounterFetch();
      preparedCounterFetch.setInt(1, event);
      ResultSet rs = preparedCounterFetch.executeQuery();
      if (!rs.next()) {
        throw new SQLException("No row found for event " + event);
      }
      int counter = rs.getInt("counter");

      // Increment the counter for the event.
      counter += 1;
      PreparedStatement preparedCounterIncrement =
          getPreparedCounterIncrement();
      preparedCounterIncrement.setInt(1, counter);
      preparedCounterIncrement.setInt(2, event);
      preparedCounterIncrement.executeUpdate();

      // Commit the transaction.
      connection.commit();

      // Detect a stale read.
      // Fetched counter after increment must be greater
      // than the cached counter. Otherwise, the read is stale.
      if (!(counter > cachedCounter)) {
        numStaleReads.incrementAndGet();
      }

      // Update the counter cache as well.
      //
      // counters tracks the maximum observed counter for each event.
      // This helps detect stale reads.
      // The new counter may be the new maximum.
      // In this case, update the cache.
      //
      // If the cached counter is higher than or equal to the
      // new counter, the new counter is no longer the maximum. Skip.
      //
      // If the cached counter is lower than the new counter,
      // we update the cache to the new counter. Do this
      // only if the cache is still at the old value. Otherwise,
      // fetch the new cached value and try again.
      // This avoids overwriting a higher cached value with counter.
      while (cachedCounter < counter && !counters.compareAndSet(
          event, cachedCounter, counter)) {
        cachedCounter = counters.get(event);
      }

      // Counter incremented successfully.
      return 1;
    } catch (Exception e) {
      try {
        connection.rollback();
      } catch (SQLException e1) {
        LOG.fatal("Failed to rollback transaction", e1);
      }
      // Suppress this error for readability.
      if (!e.getMessage().contains("Restart read required")) {
        LOG.error("Failed to increment the counter for event " + event, e);
      }
      return 0;
    }
  }

  /*
   * Appends the number of stale reads to the metrics output.
   */
  @Override
  public void appendMessage(StringBuilder sb) {
    sb.append("Stale reads: ").append(numStaleReads.get()).append(" total reads | ");
    super.appendMessage(sb);
  }
}

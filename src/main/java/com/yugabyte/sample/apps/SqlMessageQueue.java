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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

/*
 * Message queue is a common usecase in distributed systems. One such
 * application disseminates market data mesages to subscribers.
 *
 * In this app, we simulate a message queue where each message has ticker
 * and stock level information. Subscribers subscribe to a ticker and
 * accumulate the total stock. Subscribers read messages starting from
 * when they last read a message.
 *
 * This app verifies that the total stock level accumulated by a subscriber
 * is at least the stock level published by the publisher. Such a read
 * pattern is prone to read restarts because of the constant upates from
 * the publisher.
 *
 * This app helps understand whether the new clockbound clock helps
 * with this workload.
 *
 * Setup:
 * 1. Create a message_queue TABLE with columns
 *   (ticker INT, sequence_number INT, stock_level INT).
 *
 * Workload:
 * There are two operations in this workload.
 * a. Publisher: Publishes messages with a random ticker and stock level.
 * b. Subscriber: Subscribes to a ticker and accumulates the total stock
 *   of a random ticker starting from where it left off previously.
 *
 * Publisher Operation:
 * 1. Pick a random ticker.
 * 2. Pick a random stock level between 1 and 10.
 * 3. Insert the message into the message_queue TABLE with the ticker,
 *   sequence number and stock level. Sequence number is the max sequence
 *   number for the ticker + 1 or else 1.
 * 4. Increment an atomic structure with the total stock level published.
 *
 * Subscriber Operation:
 * 1. Pick a random ticker.
 * 2. Fetch the previous sequence number for the ticker.
 * 3. Fetch the stock level view of the publisher.
 * 4. Accumulate stock levels for the ticker starting from the previous
 *   sequence number into an atomic array.
 * 5. Verify that the total stock level accumulated is at least the
 *   stock level published by the publisher.
 */
public class SqlMessageQueue extends AppBase {
  private static final Logger LOG = Logger.getLogger(SqlEventCounter.class);

  // Static initialization of this app's config.
  static {
    // Use 1 Subscriber thread and 10 Publisher threads.
    appConfig.readIOPSPercentage = -1;
    appConfig.numReaderThreads = 1;
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
  private static final String DEFAULT_TABLE_NAME = "message_queue";

  // The number of tickers registered with the stock exchange.
  private static final int NUM_TICKERS = 1000;

  // The maximum stock level for a ticker published in a single message.
  private static final int MAX_STOCK_LEVEL = 10;

  // Updated by publishers whenever they publish new stock.
  private static final AtomicLongArray totalStockPublished =
      new AtomicLongArray(NUM_TICKERS);

  // The last sequence number for each ticker as seen by subscribers.
  private static final AtomicLongArray lastSequenceNumber =
      new AtomicLongArray(NUM_TICKERS);

  // Accumulated stock levels for each ticker as seen by subscribers.
  private static final AtomicLongArray totalStockAccumulated =
      new AtomicLongArray(NUM_TICKERS);

  // Number of stale reads.
  private static final AtomicLong numStaleReads = new AtomicLong(0);

  // Cache connection and statements.
  private Connection connection = null;
  private PreparedStatement preparedSubscriber = null;
  private PreparedStatement preparedPublisher = null;

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

  public PreparedStatement getPreparedSubscriber() {
    if (preparedSubscriber == null) {
      try {
        preparedSubscriber = getConnection().prepareStatement(
            String.format("SELECT MAX(sequence_number), SUM(stock_level)" +
                " FROM %s WHERE ticker = ? AND sequence_number > ?",
                getTableName()));
      } catch (Exception e) {
        LOG.fatal("Failed to prepare statement: SELECT MAX(sequence_number), SUM(stock_level)" +
            " FROM " + getTableName(), e);
      }
    }
    return preparedSubscriber;
  }

  public PreparedStatement getPreparedPublisher() {
    if (preparedPublisher == null) {
      try {
        preparedPublisher = getConnection().prepareStatement(
            String.format("INSERT INTO %s (ticker, sequence_number, stock_level)" +
                " SELECT ?, COALESCE(MAX(sequence_number), 0) + 1, ?" +
                " FROM %s WHERE ticker = ?",
                getTableName(), getTableName()));
      } catch (Exception e) {
        LOG.fatal("Failed to prepare statement: INSERT INTO " + getTableName(), e);
      }
    }
    return preparedPublisher;
  }

  @Override
  public void createTablesIfNeeded(TableOp tableOp) throws Exception {
    Connection connection = getConnection();
    // Every run should start cleanly.
    connection.createStatement().execute(
        String.format("DROP TABLE IF EXISTS %s", getTableName()));
    LOG.info("Dropping any table(s) left from previous runs if any");
    connection.createStatement().execute(String.format(
        "CREATE TABLE %s (ticker INT, sequence_number INT, stock_level INT)"
        + " SPLIT INTO 24 TABLETS",
        getTableName()));
    LOG.info(String.format("Created table: %s", getTableName()));
  }

  @Override
  public String getTableName() {
    String tableName = appConfig.tableName != null ?
        appConfig.tableName : DEFAULT_TABLE_NAME;
    return tableName.toLowerCase();
  }

  @Override
  public long doRead() {
    int ticker = ThreadLocalRandom.current().nextInt(NUM_TICKERS);

    try {
      long prevSequenceNumber = lastSequenceNumber.get(ticker);
      long stockLevelPublished = totalStockPublished.get(ticker);

      PreparedStatement preparedSubcriber = getPreparedSubscriber();
      preparedSubcriber.setInt(1, ticker);
      preparedSubcriber.setLong(2, prevSequenceNumber);
      ResultSet resultSet = preparedSubcriber.executeQuery();
      if (!resultSet.next()) {
        LOG.info("No new entries for ticker " + ticker);
        return 0;
      }
      long maxSequenceNumber = resultSet.getLong(1);
      long sumStockLevel = resultSet.getLong(2);

      if (lastSequenceNumber.compareAndSet(
          ticker, prevSequenceNumber, maxSequenceNumber)) {
        // Accumulate the stock level.
        long accumulatedStockLevel =
            totalStockAccumulated.addAndGet(ticker, sumStockLevel);
        // For some reason, this hasn't caught up with the publisher.
        if (accumulatedStockLevel < stockLevelPublished) {
          LOG.error("Stale stock level for ticker " + ticker);
          numStaleReads.incrementAndGet();
        }
        return 1;
      }

      // Someone else did the work, do not add to the total.
      return 0;
    } catch (Exception e) {
      // Suppress this error for readability.
      if (!e.getMessage().contains("Restart read required"))
      LOG.error("Error executing read query", e);
      return 0;
    }
  }

  @Override
  public long doWrite(int threadIdx) {
    int ticker = ThreadLocalRandom.current().nextInt(NUM_TICKERS);
    int stockLevel = ThreadLocalRandom.current().nextInt(MAX_STOCK_LEVEL) + 1;

    try {
      PreparedStatement preparedPublisher = getPreparedPublisher();
      preparedPublisher.setInt(1, ticker);
      preparedPublisher.setInt(2, stockLevel);
      preparedPublisher.setInt(3, ticker);
      preparedPublisher.executeUpdate();
      totalStockPublished.addAndGet(ticker, stockLevel);
      return 1;
    } catch (Exception e) {
      // Suppress this error for readability.
      if (!e.getMessage().contains("Restart read required")) {
        LOG.error("Error publishing message ", e);
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

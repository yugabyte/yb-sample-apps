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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.log4j.Logger;

/*
 * E-commerce is another important usecase for OLTP databases. TPC-C is an
 * example benchmark that simulates a simple e-commerce workload.
 *
 * Simulate restocking items in a warehouse. Customers
 * continously place orders that deplete the stock of items. The warehouse
 * restocks items whose stock has fallen below 10 items by adding a
 * 100 items. The warehouse has 100 items of each type initially.
 * There are 1000 types of items.
 *
 * The restocking operation does a full table scan to find the items that
 * have a low stock. This leads to read restart errors. This app helps
 * understand whether the new clockbound clock helps improve the
 * performance of this workload.
 *
 * Database Configuration:
 *  configure with wallclock and compare the metrics with
 *  a clockbound clock configuration.
 *
 * Setup:
 * 1. Create a warehouse_stock TABLE with columns (item_id INT, stock INT).
 * 2. Insert 100 items with item_id 0 to 999 initialized to 100 stock.
 *
 * Workload:
 * There are two operations in this workload.
 * a. NewOrder: Decrement the stock of a random item.
 * b. Restock: Restocks items whose stock has fallen below 10 items by
 *   adding 100 items.
 *
 * NewOrder Operation:
 * 1. Pick a random item_id.
 * 2. Decrement the stock of the item only when there is enough stock.
 *
 * Restock Operation:
 * 1. Scan the table, restock by 100 when below 10.
 */
public class SqlWarehouseStock extends AppBase {
  private static final Logger LOG = Logger.getLogger(SqlWarehouseStock.class);

  // Static initialization of this app's config.
  static {
    // Use 1 Restock thread and 100 NewOrder threads.
    appConfig.readIOPSPercentage = -1;
    appConfig.numReaderThreads = 1;
    appConfig.numWriterThreads = 100;
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
  private static final String DEFAULT_TABLE_NAME = "warehouse_stock";

  // The number of items in the warehouse.
  private static final int NUM_ITEMS = 1000;

  // The stock level below which restocking is needed.
  private static final int RESTOCK_THRESHOLD = 10;

  // The amount to restock by.
  private static final int RESTOCK_AMOUNT = 100;

  // Initial stock.
  private static final int INITIAL_STOCK = 100;

  // Shared counter to store the number of restocks required.
  private static final AtomicLong numRestocksRequired = new AtomicLong(0);

  // Shared counter to store the number of stale reads.
  private static final AtomicLong numStaleReads = new AtomicLong(0);

  // Cache connection and statements.
  private Connection connection = null;
  private PreparedStatement preparedRestock = null;
  private PreparedStatement preparedNewOrder = null;

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

  public PreparedStatement getPreparedRestock() {
    if (preparedRestock == null) {
      try {
        preparedRestock = getConnection().prepareStatement(
            String.format("UPDATE %s SET stock = stock + %d WHERE stock < %d",
                getTableName(), RESTOCK_AMOUNT, RESTOCK_THRESHOLD));
      } catch (Exception e) {
        LOG.fatal("Failed to prepare statement: UPDATE " + getTableName(), e);
      }
    }
    return preparedRestock;
  }

  public PreparedStatement getPreparedNewOrder() {
    if (preparedNewOrder == null) {
      try {
        preparedNewOrder = getConnection().prepareStatement(
            String.format("UPDATE %s SET stock = stock - 1" +
            " WHERE item_id = ? AND stock > 0" +
            " RETURNING stock",
            getTableName()));
      } catch (Exception e) {
        LOG.fatal("Failed to prepare statement: UPDATE " + getTableName(), e);
      }
    }
    return preparedNewOrder;
  }

  @Override
  public void createTablesIfNeeded(TableOp tableOp) throws Exception {
    Connection connection = getConnection();
    // Every run should start cleanly.
    connection.createStatement().execute(
        String.format("DROP TABLE IF EXISTS %s", getTableName()));
    LOG.info("Dropping any table(s) left from previous runs if any");
    connection.createStatement().execute(String.format(
        "CREATE TABLE %s (item_id INT PRIMARY KEY, stock INT)" +
        " SPLIT INTO 24 TABLETS",
        getTableName()));
    LOG.info(String.format("Created table: %s", getTableName()));
    int numRows = connection.createStatement().executeUpdate(String.format(
        "INSERT INTO %s SELECT GENERATE_SERIES(0, %d-1), %d",
        getTableName(), NUM_ITEMS, INITIAL_STOCK));
    LOG.info(String.format(
        "Inserted %d rows into %s", numRows, getTableName()));
  }

  @Override
  public String getTableName() {
    String tableName = appConfig.tableName != null ?
        appConfig.tableName : DEFAULT_TABLE_NAME;
    return tableName.toLowerCase();
  }

  // Executes the Restock operation.
  @Override
  public long doRead() {
    try {
      long restocksRequired = numRestocksRequired.get();
      PreparedStatement preparedRestock = getPreparedRestock();
      int numRestocked = preparedRestock.executeUpdate();
      if (numRestocked < restocksRequired) {
        numStaleReads.incrementAndGet();
      }
      numRestocksRequired.addAndGet(-numRestocked);
      return 1;
    } catch (Exception e) {
      // Suppress this error for readability.
      if (!e.getMessage().contains("Restart read required")) {
        LOG.error("Error restocking ", e);
      }
      return 0;
    }
  }

  // Executes a NewOrder operation.
  @Override
  public long doWrite(int threadIdx) {
    try {
      int itemId = ThreadLocalRandom.current().nextInt(NUM_ITEMS);
      PreparedStatement preparedNewOrder = getPreparedNewOrder();
      preparedNewOrder.setInt(1, itemId);
      ResultSet rs = preparedNewOrder.executeQuery();
      if (!rs.next()) {
        // No rows updated, return 0.
        return 0;
      }
      int stock = rs.getInt(1);
      if (stock < RESTOCK_THRESHOLD) {
        numRestocksRequired.incrementAndGet();
      }
      return 1;
    } catch (Exception e) {
      // Suppress this error for readability.
      if (!e.getMessage().contains("Restart read required")) {
        LOG.error("Error creating a new order ", e);
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

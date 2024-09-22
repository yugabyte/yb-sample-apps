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
import java.sql.Statement;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.log4j.Logger;

/*
 * Consistent hashing is useful when you have a dynamic set of nodes and
 * you need to send a key-value request to one of the nodes. Consistent
 * hashing is great at load balancing without moving too many keys when
 * nodes are added or removed.
 *
 * This app maintains a list of hashes one for each "virtual" node and
 * supports two operations:
 * a. Config change: Add or remove a node.
 * b. Get node: Get the node for a given key.
 *
 * Config Change Operation:
 * 1. At coin flip, choose whether to add or remove a node.
 * 2. If adding a node, add a node with a random hash.
 * 3. If removing a node, remove a random node.
 *
 * Get Node Operation:
 * 1. Pick a random key.
 * 2. Find the node with the smallest hash greater than the key.
 *   If no such node exists, return the smallest hash node.
 */
public class SqlConsistentHashing extends AppBase {
  private static final Logger LOG = Logger.getLogger(SqlConsistentHashing.class);

  // Static initialization of this app's config.
  static {
    // Use 10 Get Node threads and 10 Config Change threads.
    appConfig.readIOPSPercentage = -1;
    appConfig.numReaderThreads = 10;
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
  private static final String DEFAULT_TABLE_NAME = "consistent_hashing";

  // Initial number of nodes.
  private static final int INITIAL_NODES = 100000;

  // Cache connection and statements.
  private Connection connection = null;
  private PreparedStatement preparedAddNode = null;
  private PreparedStatement preparedRemoveNode = null;
  private PreparedStatement preparedGetNode = null;

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

  public PreparedStatement prepareAddNode() {
    if (preparedAddNode == null) {
      try {
        preparedAddNode = getConnection().prepareStatement(
            String.format("INSERT INTO %s (node_hash) VALUES (?)", getTableName()));
      } catch (Exception e) {
        LOG.fatal("Failed to prepare add node statement ", e);
      }
    }
    return preparedAddNode;
  }

  public PreparedStatement prepareRemoveNode() {
    if (preparedRemoveNode == null) {
      try {
        preparedRemoveNode = getConnection().prepareStatement(
          String.format("DELETE FROM %s WHERE node_hash =" +
          " (SELECT node_hash FROM %s ORDER BY RANDOM() LIMIT 1)",
          getTableName(), getTableName()));
      } catch (Exception e) {
        LOG.fatal("Failed to prepare remove node statement ", e);
      }
    }
    return preparedRemoveNode;
  }

  public PreparedStatement prepareGetNode() {
    if (preparedGetNode == null) {
      try {
        preparedGetNode = getConnection().prepareStatement(
            String.format("SELECT COALESCE(" +
            " (SELECT MIN(node_hash) FROM %s WHERE node_hash > ?)," +
            " (SELECT MIN(node_hash) FROM %s)" +
            ")",
            getTableName(), getTableName()));
      } catch (Exception e) {
        LOG.fatal("Failed to prepare get node statement ", e);
      }
    }
    return preparedGetNode;
  }

  @Override
  public void createTablesIfNeeded(TableOp tableOp) throws Exception {
    Connection connection = getConnection();
    // Every run should start cleanly.
    connection.createStatement().execute(
        String.format("DROP TABLE IF EXISTS %s", getTableName()));
    LOG.info("Dropping any table(s) left from previous runs if any");
    connection.createStatement().execute(String.format(
        "CREATE TABLE %s (node_hash INT) SPLIT INTO 24 TABLETS",
        getTableName()));
    LOG.info("Created table " + getTableName());
    connection.createStatement().execute(String.format(
        "INSERT INTO %s" +
        " SELECT (RANDOM() * 1000000000)::INT" +
        " FROM generate_series(1, %d)",
        getTableName(), INITIAL_NODES));
    LOG.info("Inserted " + INITIAL_NODES + " nodes into " + getTableName());
  }

  @Override
  public String getTableName() {
    String tableName = appConfig.tableName != null ?
        appConfig.tableName : DEFAULT_TABLE_NAME;
    return tableName.toLowerCase();
  }

  @Override
  public long doRead() {
    int key = ThreadLocalRandom.current().nextInt();
    try {
      PreparedStatement preparedGetNode = prepareGetNode();
      preparedGetNode.setInt(1, key);
      preparedGetNode.executeQuery();
      return 1;
    } catch (Exception e) {
      // Suppress this error for readability.
      if (!e.getMessage().contains("Restart read required")) {
        LOG.error("Error retrieving node uuid", e);
      }
      return 0;
    }
  }

  @Override
  public long doWrite(int threadIdx) {
    int coinFlip = ThreadLocalRandom.current().nextInt(2);
    if (coinFlip == 0) {
      return addNode();
    } else {
      return removeNode();
    }
  }

  public long addNode() {
    try {
      int nodeHash = ThreadLocalRandom.current().nextInt();
      PreparedStatement preparedAddNode = prepareAddNode();
      preparedAddNode.setInt(1, nodeHash);
      preparedAddNode.executeUpdate();
      return 1;
    } catch (Exception e) {
      // Suppress this error for readability.
      if (!e.getMessage().contains("Restart read required")) {
        LOG.error("Error adding a node " + e);
      }
      return 0;
    }
  }

  public long removeNode() {
    try {
      PreparedStatement preparedRemoveNode = prepareRemoveNode();
      preparedRemoveNode.executeUpdate();
      return 1;
    } catch (Exception e) {
      // Suppress this error for readability.
      if (!e.getMessage().contains("Restart read required")) {
        LOG.error("Error removing a node " + e);
      }
      return 0;
    }
  }
}

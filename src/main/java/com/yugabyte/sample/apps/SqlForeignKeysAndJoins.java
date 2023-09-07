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
import java.sql.Timestamp;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import com.yugabyte.sample.apps.AppBase.TableOp;
import com.yugabyte.sample.common.SimpleLoadGenerator.Key;

import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;

/**
 * This workload writes and reads some random string keys from a postgresql table.
 */
public class SqlForeignKeysAndJoins extends AppBase {
  private static final Logger LOG = Logger.getLogger(SqlForeignKeysAndJoins.class);

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
  private static final String DEFAULT_TABLE1_NAME = "SqlFKUsers";
  private static final String DEFAULT_TABLE2_NAME = "SqlFKUserOrders";

  // The initial set of users to pre-populate.
  private static final int NUM_USERS_AT_START = 1000;

  // Ratio of new user creation to num orders placed for writes. For example, if set to 10, then
  // 10% of the writes will create new users, 90% will add orders for existing users.
  private static final double USERS_TO_ORDERS_PERCENT = 10;

  // The shared prepared select statement for fetching the data.
  private volatile PreparedStatement preparedSelect = null;

  // The shared prepared insert statement for creating new users.
  private volatile PreparedStatement preparedInsertUser = null;

  // The shared prepared insert statement for creating new orders.
  private volatile PreparedStatement preparedInsertOrder = null;

  // Lock for initializing prepared statement objects.
  private static final Object prepareInitLock = new Object();

  public SqlForeignKeysAndJoins() {
    buffer = new byte[appConfig.valueSize];
  }

  /**
   * Drop the table created by this app.
   */
  @Override
  public void dropTable() throws Exception {
    try (Connection connection = getPostgresConnection()) {
      connection.createStatement().execute("DROP TABLE IF EXISTS " + getTable2Name());
      LOG.info(String.format("Dropped table: %s", getTable2Name()));
      connection.createStatement().execute("DROP TABLE IF EXISTS " + getTable1Name());
      LOG.info(String.format("Dropped table: %s", getTable1Name()));
    }
  }

  @Override
  public void createTablesIfNeeded(TableOp tableOp) throws Exception {
    try (Connection connection = getPostgresConnection()) {
      connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
  
      // (Re)Create the table (every run should start cleanly with an empty table).
      connection.createStatement().execute(
          String.format("DROP TABLE IF EXISTS %s", getTable2Name()));
      connection.createStatement().execute(
          String.format("DROP TABLE IF EXISTS %s", getTable1Name()));
      LOG.info("Dropping table(s) left from previous runs if any");
  
      try {
        // Create the "users" table.
        connection.createStatement().executeUpdate(
            String.format("CREATE TABLE %s (user_id TEXT PRIMARY KEY, user_details TEXT);", getTable1Name()));
        LOG.info(String.format("Created table: %s", getTable1Name()));
  
        // Create the "orders" table.
        connection.createStatement().executeUpdate(
            String.format("CREATE TABLE %s (" + 
                          "  user_id TEXT, " +
                          "  order_time TIMESTAMP, " +
                          "  order_id UUID DEFAULT gen_random_uuid(), " + 
                          "  order_details TEXT, " +
                          "  PRIMARY KEY (user_id, order_time DESC, order_id), " +
                          "  FOREIGN KEY (user_id) REFERENCES %s (user_id)" +
                          ");", getTable2Name(), getTable1Name()));
        LOG.info(String.format("Created table: %s", getTable2Name()));
      } catch (SQLException e) {
        LOG.error("Failed to create tables", e);
        System.err.println("\n================================================");
        System.err.println("If you hit the following:");
        System.err.println("    ERROR: function gen_random_uuid() does not exist");
        System.err.println("  Run this cmd in ysqlsh: CREATE EXTENSION \"pgcrypto\";");
        System.err.println("==================================================\n");
        System.exit(0);
      }
  
      // Pre-populate the users table with the desired number of users.
      
      for (int idx = 0; idx < NUM_USERS_AT_START; idx++) {
        insertUser();
      }
    }
  }

  public String getTable1Name() {
    return DEFAULT_TABLE1_NAME.toLowerCase();
  }

  public String getTable2Name() {
    return DEFAULT_TABLE2_NAME.toLowerCase();
  }

  private PreparedStatement getPreparedSelect() throws Exception {
    PreparedStatement preparedSelectLocal = preparedSelect;
    if (preparedSelectLocal == null) {
      synchronized (prepareInitLock) {
        if (preparedSelect == null) {
          preparedSelect = getPostgresConnection().prepareStatement(
              String.format("SELECT * FROM %s, %s " + 
                            "WHERE %s.user_id = ? AND %s.user_id = %s.user_id " + 
                            "ORDER BY order_time DESC " +
                            "LIMIT 10;", getTable1Name(), getTable2Name(),
                            getTable1Name(), getTable1Name(), getTable2Name()));
        }
        preparedSelectLocal = preparedSelect;
      }
    }
    return preparedSelectLocal;
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
/*
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
*/
        int count = 0;
        while (rs.next()) {
          ++count;
          // Get data from the current row and use it
        }
        LOG.debug("Got " + count + " orders for user : " + key.toString());
      }
    } catch (Exception e) {
      LOG.info("Failed reading value: " + key.getValueStr(), e);
      synchronized (prepareInitLock) {
        preparedSelect = null;
      }
      return 0;
    }
    return 1;
  }

  private PreparedStatement getPreparedInsertUser() throws Exception {
    PreparedStatement preparedInsertUserLocal = preparedInsertUser;
    if (preparedInsertUserLocal == null) {
      synchronized (prepareInitLock) {
        if (preparedInsertUser == null) {
          String stmt = String.format("INSERT INTO %s (user_id, user_details) VALUES (?, ?);", getTable1Name());
          if (preparedInsertUser == null) {
            Connection connection = getPostgresConnection();
            connection.createStatement().execute("set yb_enable_upsert_mode = true");
            preparedInsertUser = connection.prepareStatement(stmt);
          }
        }
        preparedInsertUserLocal = preparedInsertUser;
      }
    }
    return preparedInsertUserLocal;
  }

  private PreparedStatement getPreparedInsertUserOrder() throws Exception {
    PreparedStatement preparedInsertOrderLocal = preparedInsertOrder;
    if (preparedInsertOrderLocal == null) {
      synchronized (prepareInitLock) {
        if (preparedInsertOrder == null) {
          String stmt = String.format("INSERT INTO %s (user_id, order_time, order_details) VALUES (?, ?, ?);", 
                                      getTable2Name());
          if (preparedInsertOrder == null) {
            Connection connection = getPostgresConnection();
            connection.createStatement().execute("set yb_enable_upsert_mode = true");
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            preparedInsertOrder = connection.prepareStatement(stmt);
          }
        }
        preparedInsertOrderLocal = preparedInsertOrder;
      }
    }
    return preparedInsertOrderLocal;
  }

  @Override
  public long doWrite(int threadIdx) {
    if (random.nextInt(100) < USERS_TO_ORDERS_PERCENT) {
      return insertUser();
    }
    else {
      return insertUserOrder();
    }
  }

  private long insertUser() {
    Key key = getSimpleLoadGenerator().getKeyToWrite();
    if (key == null) {
      return 0;
    }

    int result = 0;
    try {
      PreparedStatement statement = getPreparedInsertUser();
      // Prefix hashcode to ensure generated keys are random and not sequential.
      statement.setString(1, key.asString());
      statement.setString(2, key.getValueStr());
      result = statement.executeUpdate();
      LOG.debug("Wrote key: " + key.asString() + ", " + key.getValueStr() + ", return code: " +
                result);
      getSimpleLoadGenerator().recordWriteSuccess(key);
      return 1;
    } catch (Exception e) {
      getSimpleLoadGenerator().recordWriteFailure(key);
      LOG.info("Failed writing key: " + key.asString(), e);
      synchronized (prepareInitLock) {
        preparedInsertUser = null;
      }
    }
    return 0;
  }

  private long insertUserOrder() {
    Key key = getSimpleLoadGenerator().getKeyToRead();
    if (key == null) {
      // There are no keys to read yet.
      return 0;
    }

    int result = 0;
    try {
      PreparedStatement statement = getPreparedInsertUserOrder();
      // Prefix hashcode to ensure generated keys are random and not sequential.
      statement.setString(1, key.asString());
      statement.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
      statement.setString(3, key.getValueStr());
      result = statement.executeUpdate();
      LOG.debug("Wrote key: " + key.asString() + ", " + key.getValueStr() + ", return code: " +
                result);
      getSimpleLoadGenerator().recordWriteSuccess(key);
      return 1;
    } catch (Exception e) {
      getSimpleLoadGenerator().recordWriteFailure(key);
      LOG.info("Failed writing key: " + key.asString(), e);
      synchronized (prepareInitLock) {
        preparedInsertOrder = null;
      }
    }
    return 0;
  }

  @Override
  public List<String> getWorkloadDescription() {
    return Arrays.asList(
        "Simple user orders app based on SQL. The app creates unique user ids and adds ",
        "orders for each user. The ratio of new users created and the orders entered is ",
        "configurable. On the read path, the app queries the k most recent orders per ",
        "user.");
  }

  @Override
  public List<String> getWorkloadOptionalArguments() {
    return Arrays.asList(
        "--num_unique_keys " + appConfig.numUniqueKeysToWrite,
        "--num_threads_read " + appConfig.numReaderThreads,
        "--num_threads_write " + appConfig.numWriterThreads,
        "--username " + "<DB USERNAME>",
        "--password " + "<OPTIONAL PASSWORD>",
        "--load_balance " + appConfig.loadBalance,
        "--topology_keys " + appConfig.topologyKeys,
        "--debug_driver " + appConfig.enableDriverDebug);
  }
}

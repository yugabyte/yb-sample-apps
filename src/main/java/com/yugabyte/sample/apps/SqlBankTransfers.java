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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.log4j.Logger;

/*
 * Money transfers across bank accounts is a common usecase for a OLTP
 * database. Transfers are a commonly used example for discussing
 * transactions in databases because of its strong requirements on
 * consistency guarantees.
 *
 * Simulate money transfers. The most important constraint here
 * is that the total amount of money across all accounts should remain
 * invariant. However, aggregating money across all accounts involves
 * a full table scan and this exposes the query to read restarts.
 *
 * This app helps understand whether the new clockbound clock
 * helps improve the performance of this workload.
 *
 * Database Configuration:
 *  configure with wallclock and compare the metrics with
 *  a clockbound clock configuration.
 *
 * Setup:
 * 1. Create a bank_accounts TABLE with columns (account_id INT, balance INT).
 * 2. Insert 1000 accounts with account_id 0 to 999 initialized to 1000.
 *
 * Workload:
 * There are two main operations in this workload:
 * a. Transfer: Transfers a random amount money from one account to another.
 *   The amount must be <= the balance of the source account.
 * b. Verify: Verifies that the total amount of money across all accounts
 *   is 1000 * 1000.
 *
 * Transfer Operation:
 * 1. Pick a sender and a receiver pair at random (they must be different).
 * 2. Start a repeatable read transaction.
 * 3. Query the account balance of the sender.
 * 4. If the balance is zero, abort the transaction.
 * 5. Pick a random amount [1, balance].
 * 6. Decrement the balance of the sender by the amount.
 * 7. Increment the balance of the receiver by the amount.
 * 8. Commit the transaction.
 *
 * Verify Operation:
 * 1. Sum the balances of all accounts.
 * 2. Verify that the sum is 1000 * 1000.
 */
public class SqlBankTransfers extends AppBase {
  private static final Logger LOG = Logger.getLogger(SqlBankTransfers.class);

  // Static initialization of this app's config.
  static {
    // Use 1 Verify thread and 10 Transfer threads.
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
  private static final String DEFAULT_TABLE_NAME = "bank_accounts";

  // The number of accounts in the bank.
  private static final int NUM_ACCOUNTS = 1000;

  // Initial balance of each account.
  private static final int INIT_BALANCE = 1000;

  // Shared counter to store the number of inconsistent reads.
  private static final AtomicLong numInconsistentReads = new AtomicLong(0);

  // Cache connection and statements.
  private Connection connection = null;
  private PreparedStatement preparedTotalBalance = null;
  private PreparedStatement preparedFetchSenderBalance = null;
  private PreparedStatement preparedDebitSender = null;
  private PreparedStatement preparedCreditReceiver = null;

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

  public PreparedStatement getPreparedTotalBalance() {
    if (preparedTotalBalance == null) {
      try {
        preparedTotalBalance = getConnection().prepareStatement(
            String.format("SELECT SUM(balance) FROM %s", getTableName()));
      } catch (Exception e) {
        LOG.fatal("Failed to prepare total balance statement ", e);
      }
    }
    return preparedTotalBalance;
  }

  public PreparedStatement getPreparedFetchSenderBalance() {
    if (preparedFetchSenderBalance == null) {
      try {
        preparedFetchSenderBalance = getConnection().prepareStatement(
            String.format("SELECT balance FROM %s WHERE account_id = ?",
                getTableName()));
      } catch (Exception e) {
        LOG.fatal("Failed to prepare fetch sender balance statement ", e);
      }
    }
    return preparedFetchSenderBalance;
  }

  public PreparedStatement getPreparedDebitSender() {
    if (preparedDebitSender == null) {
      try {
        preparedDebitSender = getConnection().prepareStatement(
            String.format("UPDATE %s SET balance = balance - ? WHERE account_id = ?",
                getTableName()));
      } catch (Exception e) {
        LOG.fatal("Failed to prepare debit sender statement ", e);
      }
    }
    return preparedDebitSender;
  }

  public PreparedStatement getPreparedCreditReceiver() {
    if (preparedCreditReceiver == null) {
      try {
        preparedCreditReceiver = getConnection().prepareStatement(
            String.format("UPDATE %s SET balance = balance + ? WHERE account_id = ?",
                getTableName()));
      } catch (Exception e) {
        LOG.fatal("Failed to prepare credit receiver statement ", e);
      }
    }
    return preparedCreditReceiver;
  }

  @Override
  public void createTablesIfNeeded(TableOp tableOp) throws Exception {
    Connection connection = getConnection();
    // Every run should start cleanly.
    connection.createStatement().execute(
        String.format("DROP TABLE IF EXISTS %s", getTableName()));
    LOG.info("Dropping any table(s) left from previous runs if any");
    connection.createStatement().execute(String.format(
        "CREATE TABLE %s (account_id INT, balance INT)",
        getTableName()));
    LOG.info(String.format("Created table: %s", getTableName()));
    int numRows = connection.createStatement().executeUpdate(String.format(
        "INSERT INTO %s SELECT GENERATE_SERIES(0, %d-1), %d",
        getTableName(), NUM_ACCOUNTS, INIT_BALANCE));
    LOG.info(String.format(
        "Inserted %d rows into %s", numRows, getTableName()));
  }

  @Override
  public String getTableName() {
    String tableName = appConfig.tableName != null ?
        appConfig.tableName : DEFAULT_TABLE_NAME;
    return tableName.toLowerCase();
  }

  // Executes the Verify operation.
  @Override
  public long doRead() {
    try {
      PreparedStatement preparedTotalBalance = getPreparedTotalBalance();
      ResultSet resultSet = preparedTotalBalance.executeQuery();
      if (!resultSet.next()) {
          throw new SQLException("No rows returned from sum query");
      }
      int totalBalance = resultSet.getInt(1);

      // verify total balance.
      if (totalBalance != NUM_ACCOUNTS * INIT_BALANCE) {
          LOG.error(String.format("Total balance is %d", totalBalance));
          numInconsistentReads.incrementAndGet();
      }
    } catch (Exception e) {
      // Suppress this error for readability.
      if (!e.getMessage().contains("Restart read required")) {
        LOG.error("Error verifying balances ", e);
      }
      return 0;
    }
    return 1;
  }

  // Executes the Transfer operation.
  @Override
  public long doWrite(int threadIdx) {
    // Pick two random distinct accounts.
    int sender = ThreadLocalRandom.current().nextInt(NUM_ACCOUNTS);
    int receiver;
    do {
        receiver = ThreadLocalRandom.current().nextInt(NUM_ACCOUNTS);
    } while (receiver == sender);

    Connection connection = getConnection();
    try {
      // Start a repeatable read transaction.
      connection.setAutoCommit(false);
      connection.setTransactionIsolation(
          Connection.TRANSACTION_REPEATABLE_READ);

      // Retrieve the balance of the sender.
      PreparedStatement preparedFetchSenderBalance =
          getPreparedFetchSenderBalance();
      preparedFetchSenderBalance.setInt(1, sender);
      ResultSet rs = preparedFetchSenderBalance.executeQuery();
      if (!rs.next()) {
        throw new SQLException("No row found for account " + sender);
      }
      int senderBalance = rs.getInt("balance");

      // If the sender has no money, abort the transaction.
      if (senderBalance <= 0) {
        if (senderBalance < 0) {
          LOG.error(String.format(
              "Sender %d has negative balance %d", sender, senderBalance));
          numInconsistentReads.incrementAndGet();
        }
        throw new SQLException("Sender has no money");
      }

      // Pick a random amount to transfer [1, sendBalance].
      int amount = ThreadLocalRandom.current().nextInt(1, senderBalance + 1);

      // Decrement the sender's balance.
      PreparedStatement preparedDebitSender = getPreparedDebitSender();
      preparedDebitSender.setInt(1, amount);
      preparedDebitSender.setInt(2, sender);
      preparedDebitSender.executeUpdate();

      // Increment the receiver's balance.
      PreparedStatement preparedCreditReceiver = getPreparedCreditReceiver();
      preparedCreditReceiver.setInt(1, amount);
      preparedCreditReceiver.setInt(2, receiver);
      preparedCreditReceiver.executeUpdate();

      // Commit the transaction.
      connection.commit();

      // Transfer successful.
      return 1;
    } catch (Exception e) {
      try {
        connection.rollback();
      } catch (Exception e1) {
        LOG.fatal("Error rolling back transaction ", e1);
      }
      // Suppress this error for readability.
      if (!e.getMessage().contains("Restart read required")) {
        LOG.error("Error transferring money ", e);
      }
      return 0;
    }
  }

  /*
   * Appends the number of inconsistent reads to the metrics output.
   */
  @Override
  public void appendMessage(StringBuilder sb) {
    sb.append("Inconsistent reads: ").append(
        numInconsistentReads.get()).append(" total reads | ");
    super.appendMessage(sb);
  }
}

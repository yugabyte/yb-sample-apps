package com.yugabyte.sample.apps.anomalies;

import static com.yugabyte.sample.apps.AppBase.close;

import com.yugabyte.sample.apps.AppBase;
import com.yugabyte.sample.apps.SqlInserts;
import com.yugabyte.sample.common.CmdLineOpts;
import com.yugabyte.sample.common.CmdLineOpts.ContactPoint;
import com.yugabyte.sample.common.SimpleLoadGenerator.Key;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.Logger;

public class SqlInsertTablets extends SqlInserts {
  private static final Logger LOG = Logger.getLogger(SqlInsertTablets.class);

  public static CyclicBarrier readBarrier;
  public static CyclicBarrier writeBarrier;
  public static AtomicBoolean setupLock = new AtomicBoolean(false);

  @Override
  public void initialize(CmdLineOpts configuration) {
    LOG.info("Initializing workload " + this.getClass().getSimpleName());

    boolean shouldSetup = setupLock.compareAndSet(false, true);
    if (shouldSetup) {
      LOG.info("Setting up iteration synchronization barriers");
      readBarrier = new CyclicBarrier(configuration.getNumReaderThreads());
      writeBarrier = new CyclicBarrier(configuration.getNumWriterThreads());
    }
  }

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
    appConfig.numKeysToRead = NUM_KEYS_TO_READ_FOR_YSQL_AND_YCQL;
    // The number of keys to write. This is the combined total number of inserts and updates.
    appConfig.numKeysToWrite = NUM_KEYS_TO_WRITE_FOR_YSQL_AND_YCQL;
    // The number of unique keys to write. This determines the number of inserts (as opposed to
    // updates).
    appConfig.numUniqueKeysToWrite = NUM_UNIQUE_KEYS_FOR_YSQL_AND_YCQL;
  }

  // The default table name to create and use for CRUD ops.
  private static final String DEFAULT_TABLE_NAME = "PostgresqlKeyValue";

  // The shared prepared select statement for fetching the data.
  public volatile Connection selConnection = null;
  public volatile PreparedStatement preparedSelect = null;

  // The shared prepared insert statement for inserting the data.
  public volatile Connection insConnection = null;
  public volatile PreparedStatement preparedInsert = null;

  public SqlInsertTablets() {
    buffer = new byte[appConfig.valueSize];
  }

  public static Hashtable<ContactPoint, Integer> contactPointUsage = new Hashtable<>();

  public Connection getPostgresConnectionFair() {
    // under a lock
    //   initialize a Hash<ContactPoint, int> that keeps track of
    //     the number of times each contact point has been used
    //   find the smallest number
    //   create a connection for that endpoint
    //   increment the number of times that endpoint has been used
    synchronized (SqlInsertTablets.class) {
      if (contactPointUsage.isEmpty()) {
        LOG.info("Initializing contact point usage");
        for (ContactPoint contactPoint : configuration.contactPoints) {
          contactPointUsage.put(contactPoint, 0);
        }
      }

      int minUsage = Integer.MAX_VALUE;
      ContactPoint minContactPoint = null;
      for (ContactPoint contactPoint : contactPointUsage.keySet()) {
        int usage = contactPointUsage.get(contactPoint);
        // LOG.info("Contact point: " + contactPoint.getHost() + " usage: " + usage);
        if (usage < minUsage) {
          minUsage = usage;
          minContactPoint = contactPoint;
        }
      }

      try {
        String dbName = appConfig.defaultPostgresDatabase;
        String dbUser = appConfig.dbUsername;
        String dbPass = appConfig.dbPassword;

        // Use the PG driver
        Class.forName("org.postgresql.Driver");

        Properties props = new Properties();
        props.setProperty("user", dbUser);
        props.setProperty("password", dbPass);
        if (appConfig.enableDriverDebug) {
          props.setProperty("loggerLevel", "debug");
        }
        props.setProperty("reWriteBatchedInserts", "true");

        String connectStr =
            String.format(
                "%s//%s:%d/%s",
                "jdbc:postgresql:", minContactPoint.getHost(), minContactPoint.getPort(), dbName);

        LOG.info("Establishing connection to host: " + connectStr);
        Connection connection = DriverManager.getConnection(connectStr, props);

        contactPointUsage.put(minContactPoint, minUsage + 1);
        return connection;
      } catch (Exception e) {
        LOG.error("Failed to get connection to " + minContactPoint, e);
        return null;
      }
    }
  }

  @Override
  public void createTablesIfNeeded(TableOp tableOp) throws Exception {
    try (Connection connection = getPostgresConnection()) {

      // (Re)Create the table (every run should start cleanly with an empty table).
      if (tableOp.equals(TableOp.DropTable)) {
        connection
            .createStatement()
            .execute(String.format("DROP TABLE IF EXISTS %s", getTableName()));
        LOG.info("Dropping any table(s) left from previous runs if any");
      }
      connection
          .createStatement()
          .execute(
              String.format(
                  "CREATE TABLE IF NOT EXISTS %s (k text PRIMARY KEY, v text) SPLIT INTO 3 TABLETS",
                  getTableName()));
      LOG.info(String.format("Created table: %s", getTableName()));
      if (tableOp.equals(TableOp.TruncateTable)) {
        connection.createStatement().execute(String.format("TRUNCATE TABLE %s", getTableName()));
        LOG.info(String.format("Truncated table: %s", getTableName()));
      }
    }
  }

  public String getTableName() {
    String tableName = appConfig.tableName != null ? appConfig.tableName : DEFAULT_TABLE_NAME;
    return tableName.toLowerCase();
  }

  public PreparedStatement getPreparedSelect() throws Exception {
    if (preparedSelect == null) {
      LOG.info("Preparing SELECT statement");
      close(selConnection);
      selConnection = getPostgresConnectionFair();
      preparedSelect =
          selConnection.prepareStatement(
              String.format("SELECT k, v FROM %s WHERE k = ?;", getTableName()));
    }
    return preparedSelect;
  }

  @Override
  public long doRead() {
    try {
      return doReadNoBarrier(getSimpleLoadGenerator().getKeyToRead());
    } finally {
      try {
        readBarrier.await(1000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
        LOG.error("Read barrier - expected for last iteration", e);
      }
    }
  }

  public long doReadNoBarrier(Key key) {
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
      close(preparedSelect);
      preparedSelect = null;
      return 0;
    }
    return 1;
  }

  public PreparedStatement getPreparedInsert() throws Exception {
    if (preparedInsert == null) {
      LOG.info("Preparing INSERT statement");
      close(insConnection);
      insConnection = getPostgresConnectionFair();
      preparedInsert =
          insConnection.prepareStatement(
              String.format("INSERT INTO %s (k, v) VALUES (?, ?);", getTableName()));
    }
    return preparedInsert;
  }

  @Override
  public long doWrite(int threadIdx) {
    try {
      Key key = getSimpleLoadGenerator().getKeyToWrite();
      long a = doWriteNoBarrier(threadIdx, key);
      getSimpleLoadGenerator().recordWriteSuccess(key);
      return a;
    } finally {
      try {
        writeBarrier.await(1000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
        LOG.error("Write barrier - expected for last iteration", e);
      }
    }
  }

  public long doWriteNoBarrier(int threadIdx, Key key) {
    if (key == null) {
      return 0;
    }

    int result = 0;
    try {
      PreparedStatement statement = getPreparedInsert();
      // Prefix hashcode to ensure generated keys are random and not sequential.
      statement.setString(1, key.asString());
      statement.setString(2, key.getValueStr());
      result = statement.executeUpdate();
      LOG.debug(
          "Wrote key: " + key.asString() + ", " + key.getValueStr() + ", return code: " + result);
    } catch (Exception e) {
      getSimpleLoadGenerator().recordWriteFailure(key);
      LOG.info("Failed writing key: " + key.asString(), e);
      close(preparedInsert);
      preparedInsert = null;
    }
    return result;
  }

  @Override
  public List<String> getWorkloadDescription() {
    return Arrays.asList(
        "Sample key-value app built on PostgreSQL with concurrent readers and writers. The app"
            + " inserts unique string keys",
        "each with a string value to a postgres table with an index on the value column.",
        "There are multiple readers and writers that update these keys and read them",
        "for a specified number of operations,default value for read ops is "
            + AppBase.appConfig.numKeysToRead
            + " and write ops is "
            + AppBase.appConfig.numKeysToWrite
            + ", with the readers query the keys by the associated values that are",
        "indexed. Note that the number of reads and writes to perform can be specified as",
        "a parameter, user can run read/write(both) operations indefinitely by passing -1 to"
            + " --num_reads or --num_writes or both.");
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

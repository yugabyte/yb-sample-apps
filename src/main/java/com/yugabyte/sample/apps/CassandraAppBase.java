package com.yugabyte.sample.apps;

import com.datastax.driver.core.*;
import com.yugabyte.sample.common.SimpleLoadGenerator;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * Base class for all workloads that are based on key value tables.
 */
public abstract class CassandraAppBase extends AppBase {
  private static final Logger LOG = Logger.getLogger(CassandraAppBase.class);

  // Static initialization of this workload's config. These are good defaults for getting a decent
  // read dominated workload on a reasonably powered machine. Exact IOPS will of course vary
  // depending on the machine and what resources it has to spare.
  static {
    // Disable the read-write percentage.
    appConfig.readIOPSPercentage = -1;
    // Set the read and write threads to 1 each.
    appConfig.numReaderThreads = 24;
    appConfig.numWriterThreads = 2;
    // The number of keys to read.
    appConfig.numKeysToRead = -1;
    // The number of keys to write. This is the combined total number of inserts and updates.
    appConfig.numKeysToWrite = -1;
    // The number of unique keys to write. This determines the number of inserts (as opposed to
    // updates).
    appConfig.numUniqueKeysToWrite = NUM_UNIQUE_KEYS;
  }

  // The shared prepared select statement for fetching the data.
  private static volatile PreparedStatement preparedSelect;

  // The shared prepared statement for inserting into the table.
  private static volatile PreparedStatement preparedInsert;

  // Lock for initializing prepared statement objects.
  private static final Object prepareInitLock = new Object();

  public CassandraAppBase() {
    buffer = new byte[appConfig.valueSize];
  }

  /**
   * Drop the table created by this app.
   */
  @Override
  public void dropTable() {
    dropCassandraTable(getTableName());
  }

  protected PreparedStatement getPreparedSelect(String selectStmt, boolean localReads)  {
    if (preparedSelect == null) {
      synchronized (prepareInitLock) {
        if (preparedSelect == null) {
          // Create the prepared statement object.
          preparedSelect = getCassandraClient().prepare(selectStmt);
          if (localReads) {
            LOG.debug("Doing local reads");
            preparedSelect.setConsistencyLevel(ConsistencyLevel.ONE);
          }
        }
      }
    }
    return preparedSelect;
  }

  protected abstract String getDefaultTableName();

  public String getTableName() {
    return appConfig.tableName != null ? appConfig.tableName : getDefaultTableName();
  }

  @Override
  public synchronized void resetClients() {
    synchronized (prepareInitLock) {
      preparedInsert = null;
      preparedSelect = null;
    }
    super.resetClients();
  }

  @Override
  public synchronized void destroyClients() {
    synchronized (prepareInitLock) {
      preparedInsert = null;
      preparedSelect = null;
    }
    super.destroyClients();
  }

  protected PreparedStatement getPreparedInsert(String insertStmt)  {
    if (preparedInsert == null) {
      synchronized (prepareInitLock) {
        if (preparedInsert == null) {
          // Create the prepared statement object.
          preparedInsert = getCassandraClient().prepare(insertStmt);
        }
      }
    }
    return preparedInsert;
  }

  public void appendParentMessage(StringBuilder sb) {
    super.appendMessage(sb);
  }

  @Override
  public List<String> getExampleUsageOptions() {
    return Arrays.asList(
        "--num_unique_keys " + appConfig.numUniqueKeysToWrite,
        "--num_reads " + appConfig.numKeysToRead,
        "--num_writes " + appConfig.numKeysToWrite,
        "--value_size " + appConfig.valueSize,
        "--num_threads_read " + appConfig.numReaderThreads,
        "--num_threads_write " + appConfig.numWriterThreads,
        "--table_ttl_seconds " + appConfig.tableTTLSeconds);
  }
}

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

import org.apache.log4j.Logger;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This workload writes and reads some random string keys from a postgresql table.
 */
public class MultiTableMultiIndexInserts extends AppBase {
  private static final Logger LOG = Logger.getLogger(MultiTableMultiIndexInserts.class);

  // Static initialization of this workload's config.
  static {
    appConfig.readIOPSPercentage = -1;
    appConfig.numReaderThreads = 0;
    appConfig.numWriterThreads = 2;
    appConfig.numKeysToRead = -1;
    appConfig.numKeysToWrite = -1;
  }

  // Atomic counters to get unique integers and longs needed by the threads.
  private static final AtomicInteger atomicIntCounter = new AtomicInteger(0);
  private static final AtomicLong atomicLongCounter = new AtomicLong(0);

  class PerThreadState {
    private Map<String, PreparedStatement> preparedStatements = new HashMap<>();
    private final Connection connection;
    private final Random random = new Random();

    // The following variables are cached from the global atomic incrementing variable so that we
    // don't need to use an expensive atomic operation to get a single value. Each thread caches
    // 'COUNTER_CACHE' integers at a time. The 'uncached' variables indicate the excluded upper
    // bound of the cached integers.
    private int nextInt;
    private int nextIntUncached;
    private long nextLong;
    private long nextLongUncached;
    private static final int COUNTER_CACHE = 10000;

    PerThreadState(Connection conn, Map<String, PreparedStatement> preparedStatements) {
      this.connection = conn;
      this.preparedStatements = preparedStatements;

      nextInt = 0;
      nextIntUncached = 0;
      nextLong = 0;
      nextLongUncached = 0;
    }

    long getNextLong() {
      if (nextLong >= nextLongUncached) {
        nextLong = atomicLongCounter.getAndAdd(COUNTER_CACHE);
        nextLongUncached = nextLong + COUNTER_CACHE;
      }
      return nextLong++;
    }

    int getNextInt() {
      if (nextInt >= nextIntUncached) {
        nextInt = atomicIntCounter.getAndAdd(COUNTER_CACHE);
        nextIntUncached = nextInt + COUNTER_CACHE;
      }
      return nextInt++;
    }

    PreparedStatement getPreparedStatement(String tableName) {
      return preparedStatements.get(tableName);
    }

    Connection getConnection() {
      return connection;
    }

    Random getRandom() {
      return random;
    }
  }
  private static final Map<Integer, PerThreadState> threadStates = new HashMap<>();

  // Map containing the table name and the corresponding rows to be inserted as a batch.
  private static final Map<String, Integer> rowsCount = new HashMap<>();
  // Map containing the table name and the corresponding count of columns present per row.
  private static final Map<String, Integer> valuesPerRow = new HashMap<>();
  // List containing all the base table names.
  private static final List<String> baseTableNames = new LinkedList<>();
  // Map containing the table name and the corresponding list of indexes that need to be created.
  private static final Map<String, List<String>> indexes = new HashMap<>();

  public MultiTableMultiIndexInserts() {
    buffer = new byte[appConfig.valueSize];
    rowsCount.put("a1", 32);
    rowsCount.put("a1a", 256);
    rowsCount.put("a2", 8);
    rowsCount.put("d", 16);
    rowsCount.put("a3", 32);
    rowsCount.put("a3a", 1);
    rowsCount.put("m", 1);
    rowsCount.put("mv", 1);

    valuesPerRow.put("a1", 5);
    valuesPerRow.put("a1a", 4);
    valuesPerRow.put("a2", 10);
    valuesPerRow.put("d", 3);
    valuesPerRow.put("a3", 4);
    valuesPerRow.put("a3a", 4);
    valuesPerRow.put("m", 4);
    valuesPerRow.put("mv", 2);

    baseTableNames.add("a1");
    baseTableNames.add("a1a");
    baseTableNames.add("a2");
    baseTableNames.add("d");
    baseTableNames.add("a3");
    baseTableNames.add("a3a");
    baseTableNames.add("m");
    baseTableNames.add("mv");

    List<String> a1Indexes = new LinkedList<>();
    a1Indexes.add("t");
    a1Indexes.add("f_id_1");
    a1Indexes.add("f_id_2");
    indexes.put("a1", a1Indexes);

    List<String> a1aIndexes = new LinkedList<>();
    a1aIndexes.add("t");
    a1aIndexes.add("id");
    a1aIndexes.add("a2_id");
    indexes.put("a1a", a1aIndexes);

    List<String> a2Indexes = new LinkedList<>();
    a2Indexes.add("t");
    indexes.put("a2", a2Indexes);

    List<String> a3Indexes = new LinkedList<>();
    a3Indexes.add("t");
    indexes.put("a3", a3Indexes);

    List<String> a3aIndexes = new LinkedList<>();
    a3aIndexes.add("t");
    a3aIndexes.add("a3_id");
    a3aIndexes.add("a2_id");
    indexes.put("a3a", a3aIndexes);

    List<String> dIndexes = new LinkedList<>();
    dIndexes.add("t");
    dIndexes.add("mv_id");
    indexes.put("d", dIndexes);

    List<String> mIndexes = new LinkedList<>();
    mIndexes.add("m_id");
    indexes.put("m", mIndexes);
  }

  private String getTableName(String baseTableName) {
    return "Schema1_" + baseTableName;
  }

  private String getIndexName(String indexName, String baseTableName) {
    return "Schema1_" + indexName + "_" + baseTableName + "_idx";
  }

  private String getValuesString(int valuesPerRow) {
    StringBuilder builder = new StringBuilder();
    builder.append("(");
    for (int j = 0; j < valuesPerRow; ++j) {
      if (j == valuesPerRow - 1) {
        builder.append("?");
      } else {
        builder.append("?, ");
      }
    }
    builder.append(")");
    return builder.toString();
  }

  private String getA2ValuesString() {
    return "(?, ?::json, ?, ?, ?, ?, ?, ?, ?, ?)";
  }

  private PreparedStatement getPreparedInsert(Connection conn,
                                              String baseTableName) throws Exception {
    String tableName = getTableName(baseTableName);
    String valuesString;
    if (baseTableName.equals("a2")) {
      valuesString = getA2ValuesString();
    } else {
      valuesString = getValuesString(valuesPerRow.get(baseTableName));
    }
    return conn.prepareStatement(String.format("INSERT INTO %s VALUES %s;", tableName,
        valuesString));
  }

  @Override
  public void initializeConnectionsAndStatements(int numThreads) {
    for (int i = 0; i < numThreads; ++i) {
      try {
        Connection conn = getPostgresConnection();
        Map<String, PreparedStatement> m = new HashMap<>();

        for (String baseTableName : baseTableNames) {
          m.put(getTableName(baseTableName), getPreparedInsert(conn, baseTableName));
        }
        threadStates.put(i, new PerThreadState(conn, m));
      } catch (Exception e) {
        LOG.error("Statement creation failed with error ", e);
      }
    }
  }

  public void dropTable() throws Exception {
    try (Connection connection = getPostgresConnection()) {
      for (String baseTableName : baseTableNames) {
        connection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s",
            getTableName(baseTableName)));
      }
      LOG.info("Dropped all tables");
    }
  }

  @Override
  public void createTablesIfNeeded(TableOp dropTable) throws Exception {
    dropTable();
    try (Connection connection = getPostgresConnection()) {
      Statement statement = connection.createStatement();
  
      statement.execute(String.format("CREATE TABLE %s (id uuid primary key, f_id_1 uuid not null, " +
          "f_id_2 uuid not null, t integer not null, " +
          "seq integer not null);", getTableName("a1")));
      statement.execute(String.format("CREATE TABLE %s (id uuid NOT NULL, a2_id uuid NOT NULL, " +
              "t integer NOT NULL, seq integer NOT NULL);",
          getTableName("a1a")));
      statement.execute(String.format(
          "CREATE TABLE %s (id uuid primary key, json jsonb, t integer, " +
              "d_ids integer[] NOT NULL, p_t integer[] NOT NULL, " +
              "a_t integer[] NOT NULL, l integer[] NOT NULL, " +
              "jsons integer[] NOT NULL, seqs integer[] NOT NULL, " +
              "sto_ids integer[] NOT NULL);", getTableName("a2")));
      statement.execute(String.format("CREATE TABLE %s (id integer primary key, " +
              "t character varying(100) NOT NULL, mv_id integer NOT NULL);",
          getTableName("d")));
      statement.execute(String.format("CREATE TABLE %s (id uuid primary key, t integer NOT NULL, " +
              "geometry_full_geom TEXT, geometry_index TEXT);",
          getTableName("a3")));
      statement.execute(String.format("CREATE TABLE %s (a3_id uuid NOT NULL, a2_id uuid NOT NULL, " +
          "t integer NOT NULL, seq integer NOT NULL);", getTableName("a3a")));
      statement.execute(String.format("CREATE TABLE %s (m_id uuid NOT NULL, a_id text, " +
          "m_key text NOT NULL, m_value text);", getTableName("m")));
      statement.execute(String.format("CREATE TABLE %s (id integer primary key, " +
          "v character varying(30) NOT NULL);", getTableName("mv")));
      LOG.info("Created all the tables");
  
      for (Map.Entry<String, List<String>> entry : indexes.entrySet()) {
        String baseTableName = entry.getKey();
        for (String index : entry.getValue()) {
          statement.execute(String.format("CREATE INDEX %s ON %s (%s)",
              getIndexName(index, baseTableName),
              getTableName(baseTableName),
              index));
        }
      }
      LOG.info("Created all the indexes");
    }
  }

  public long doRead() {
    return 1L;
  }

  private UUID getNextUUID(int threadIdx) {
    PerThreadState threadState = threadStates.get(threadIdx);
    long a = threadState.getNextLong();
    long b = threadState.getNextLong();
    return new UUID(a, b);
  }

  private int getNextInt(int threadIdx) {
    PerThreadState threadState = threadStates.get(threadIdx);
    return threadState.getNextInt();
  }

  private PreparedStatement getPreparedStatement(int threadIdx, String baseTableName) {
    PerThreadState threadState = threadStates.get(threadIdx);
    return threadState.getPreparedStatement(getTableName(baseTableName));
  }

  private Connection getConnection(int threadIdx) {
    PerThreadState threadState = threadStates.get(threadIdx);
    return threadState.getConnection();
  }

  private static final String ALPHA_NUMERIC_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

  String getRandomString(int threadIdx) {
    Random random = threadStates.get(threadIdx).getRandom();
    StringBuilder builder = new StringBuilder();
    int count = 7;
    while (count-- != 0) {
      int character = random.nextInt(ALPHA_NUMERIC_STRING.length());
      builder.append(ALPHA_NUMERIC_STRING.charAt(character));
    }
    return builder.toString();
  }

  private PreparedStatement getA1Statement(int threadIdx) throws Exception {
    PreparedStatement preparedStatement = getPreparedStatement(threadIdx, "a1");
    int count = rowsCount.get("a1");
    for (int i = 0; i < count; ++i) {
      preparedStatement.setObject(1, getNextUUID(threadIdx));
      preparedStatement.setObject(2, getNextUUID(threadIdx));
      preparedStatement.setObject(3, getNextUUID(threadIdx));
      preparedStatement.setInt(4, getNextInt(threadIdx));
      preparedStatement.setInt(5, getNextInt(threadIdx));
      preparedStatement.addBatch();
    }

    return preparedStatement;
  }

  private PreparedStatement getA1AStatement(int threadIdx) throws Exception {
    PreparedStatement preparedStatement = getPreparedStatement(threadIdx, "a1a");
    int count = rowsCount.get("a1a");
    for (int i = 0; i < count; ++i) {
      preparedStatement.setObject(1, getNextUUID(threadIdx));
      preparedStatement.setObject(2, getNextUUID(threadIdx));
      preparedStatement.setInt(3, getNextInt(threadIdx));
      preparedStatement.setInt(4, getNextInt(threadIdx));
      preparedStatement.addBatch();
    }

    return preparedStatement;
  }

  private PreparedStatement getA2Statement(int threadIdx) throws Exception {
    PreparedStatement preparedStatement = getPreparedStatement(threadIdx, "a2");
    Connection conn = getConnection(threadIdx);
    int count = rowsCount.get("a2");
    for (int i = 0; i < count; ++i) {
      preparedStatement.setObject(1, getNextUUID(threadIdx));
      preparedStatement.setString(2, "\"abcde:abcdefghij\"");
      preparedStatement.setInt(3, getNextInt(threadIdx));

      for (int j = 4; j <= 10; ++j) {
        Integer[] arr = new Integer[0];
        Array sqlArr = conn.createArrayOf("int", arr);
        preparedStatement.setArray(j, sqlArr);
      }

      preparedStatement.addBatch();
    }

    return preparedStatement;
  }

  private PreparedStatement getDStatement(int threadIdx) throws Exception {
    PreparedStatement preparedStatement = getPreparedStatement(threadIdx, "d");
    int count = rowsCount.get("d");
    for (int i = 0; i < count; ++i) {
      preparedStatement.setInt(1, getNextInt(threadIdx));
      preparedStatement.setString(2, getRandomString(threadIdx));
      preparedStatement.setInt(3, getNextInt(threadIdx));
      preparedStatement.addBatch();
    }

    return preparedStatement;
  }

  private PreparedStatement getA3Statement(int threadIdx) throws Exception {
    PreparedStatement preparedStatement = getPreparedStatement(threadIdx, "a3");
    int count = rowsCount.get("a3");
    for (int i = 0; i < count; ++i) {
      preparedStatement.setObject(1, getNextUUID(threadIdx));
      preparedStatement.setInt(2, getNextInt(threadIdx));
      preparedStatement.setString(3, "");
      preparedStatement.setString(4,
          "0101000020E61000002A05DD5E525953C0F74773BF9EA54640");
      preparedStatement.addBatch();
    }

    return preparedStatement;
  }

  private PreparedStatement getA3AStatement(int threadIdx) throws Exception {
    PreparedStatement preparedStatement = getPreparedStatement(threadIdx, "a3a");
    int count = rowsCount.get("a3a");
    for (int i = 0; i < count; ++i) {
      preparedStatement.setObject(1, getNextUUID(threadIdx));
      preparedStatement.setObject(2, getNextUUID(threadIdx));
      preparedStatement.setInt(3, getNextInt(threadIdx));
      preparedStatement.setInt(4, getNextInt(threadIdx));
      preparedStatement.addBatch();
    }

    return preparedStatement;
  }

  private PreparedStatement getMStatement(int threadIdx) throws Exception {
    PreparedStatement preparedStatement = getPreparedStatement(threadIdx, "m");
    int count = rowsCount.get("m");
    for (int i = 0; i < count; ++i) {
      preparedStatement.setObject(1, getNextUUID(threadIdx));
      preparedStatement.setString(2, "abcdefghij");
      preparedStatement.setString(3, "abcdefghij");
      preparedStatement.setString(4, "abcdefghij");
      preparedStatement.addBatch();
    }

    return preparedStatement;
  }

  private PreparedStatement getMVStatement(int threadIdx) throws Exception {
    PreparedStatement preparedStatement = getPreparedStatement(threadIdx, "mv");
    int count = rowsCount.get("mv");
    for (int i = 0; i < count; ++i) {
      preparedStatement.setInt(1, getNextInt(threadIdx));
      preparedStatement.setString(2, "abcdefghij");
      preparedStatement.addBatch();
    }

    return preparedStatement;
  }

  public long doWrite(int threadIdx) {
    try {
      getA1Statement(threadIdx).executeBatch();
      getA1AStatement(threadIdx).executeBatch();
      getA2Statement(threadIdx).executeBatch();
      getDStatement(threadIdx).executeBatch();
      getA3Statement(threadIdx).executeBatch();
      getA3AStatement(threadIdx).executeBatch();
      getMStatement(threadIdx).executeBatch();
      getMVStatement(threadIdx).executeBatch();
      return 1;
    } catch (Exception e) {
      LOG.error("Write failed with error ", e);
      return 0;
    }
  }

  public List<String> getWorkloadDescription() {
    return Arrays.asList("Sample app for a multi table and multi index insert workload");
  }

  public List<String> getWorkloadOptionalArguments() {
    return Arrays.asList("--num_unique_keys " + appConfig.numUniqueKeysToWrite,
        "--num_reads " + appConfig.numKeysToRead,
        "--num_writes " + appConfig.numKeysToWrite,
        "--num_threads_read " + appConfig.numReaderThreads,
        "--num_threads_write " + appConfig.numWriterThreads);
  }
}

// Copyright (c) YugaByte, Inc.  //
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

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

/**
 * This workload writes and reads some random string keys from a postgresql table.
 */
public class MultiTableMultiIndexInserts extends AppBase {
  private static final Logger LOG = Logger.getLogger(MultiTableMultiIndexInserts.class);

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
    appConfig.numKeysToRead = -1;
    // The number of keys to write. This is the combined total number of inserts and updates.
    appConfig.numKeysToWrite = -1;
    // The number of unique keys to write. This determines the number of inserts (as opposed to
    // updates).
    appConfig.numUniqueKeysToWrite = NUM_UNIQUE_KEYS;
  }

  private static final Map<String, Integer> rowsCount = new HashMap<>();
  private static final Map<String, Integer> valuesPerRow = new HashMap<>();

  private static final Map<Integer, Map<String, PreparedStatement>> preparedStatements = new HashMap<>();
  private static final Map<Integer, Connection> connectionMap = new HashMap<>();

  private static final Map<Integer, Integer> nextInt = new HashMap<>();
  private static final Map<Integer, Integer> nextIntMax = new HashMap<>();
  private static final Map<Integer, Long> nextLong = new HashMap<>();
  private static final Map<Integer, Long> nextLongMax = new HashMap<>();

  private static final AtomicInteger atomicInt = new AtomicInteger(0);
  private static final AtomicLong atomicLong = new AtomicLong(0);

  int kAtomicDelta = 10000;

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
  }

  private String getTableName(String baseTableName) { return "Schema1_" + baseTableName; }

  private String getValuesString(int valuesPerRow) {
    String values = "(";
    for(int j = 0; j < valuesPerRow; ++j) {
      if (j == valuesPerRow - 1) {
        values = values + "?";
      } else {
        values = values + "?, ";
      }
    }
    values = values + ")";
    return values;
  }
  private String getA2ValuesString() { return "(?, ?::json, ?, ?, ?, ?, ?, ?, ?, ?)"; }

  private PreparedStatement getPreparedInsert(Connection conn, String baseTableName) throws Exception {
    String tableName = getTableName(baseTableName);
    String valuesString;
    if (baseTableName.equals("a2")) {
      valuesString = getA2ValuesString();
    } else {
      valuesString = getValuesString(valuesPerRow.get(baseTableName));
    }
    return conn.prepareStatement(String.format("INSERT INTO %s VALUES %s;", tableName, valuesString));
  }

  public void Initialize(int numThreads) {

    for(int i = 0; i < numThreads; ++i) {
      try {
        Connection conn = getPostgresConnection();
        connectionMap.put(i, conn);

        nextInt.put(i, 1);
        nextIntMax.put(i, 0);
        nextLong.put(i, 1L);
        nextLongMax.put(i, 0L);

        preparedStatements.put(i, new HashMap<>());
        preparedStatements.get(i).put(getTableName("a1"), getPreparedInsert(conn, "a1"));
        preparedStatements.get(i).put(getTableName("a1a"), getPreparedInsert(conn, "a1a"));
        preparedStatements.get(i).put(getTableName("a2"), getPreparedInsert(conn, "a2"));
        preparedStatements.get(i).put(getTableName("d"), getPreparedInsert(conn, "d"));
        preparedStatements.get(i).put(getTableName("a3"), getPreparedInsert(conn, "a3"));
        preparedStatements.get(i).put(getTableName("a3a"), getPreparedInsert(conn, "a3a"));
        preparedStatements.get(i).put(getTableName("m"), getPreparedInsert(conn, "m"));
        preparedStatements.get(i).put(getTableName("mv"), getPreparedInsert(conn, "mv"));
      } catch (Exception e) {
        LOG.error("Couldn't create the statements" + e);
      }
    }
  }

  public void dropTable() throws Exception {
    Connection connection = getPostgresConnection();
    connection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s", getTableName("a1")));
    connection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s", getTableName("a1a")));
    connection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s", getTableName("a2")));
    connection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s", getTableName("d")));
    connection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s", getTableName("a3")));
    connection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s", getTableName("a3a")));
    connection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s", getTableName("m")));
    connection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s", getTableName("mv")));

    LOG.info("Dropped all tables");
  }

  public void createTablesIfNeeded() throws Exception {
    dropTable();
    Connection connection = getPostgresConnection();
    Statement statement = connection.createStatement();

    statement.execute(String.format("CREATE TABLE %s (id uuid primary key, f_id_1 uuid not null, f_id_2 uuid not null, t integer not null, seq integer not null);", getTableName("a1")));
    statement.execute(String.format("CREATE TABLE %s (id uuid NOT NULL, a2_id uuid NOT NULL, t integer NOT NULL, seq integer NOT NULL);", getTableName("a1a")));
    statement.execute(String.format("CREATE TABLE %s (id uuid primary key, json jsonb,t integer, d_ids integer[] NOT NULL, p_t integer[] NOT NULL, a_t integer[] NOT NULL, l integer[] NOT NULL, jsons integer[] NOT NULL, seqs integer[] NOT NULL, sto_ids integer[] NOT NULL);", getTableName("a2")));
    statement.execute(String.format("CREATE TABLE %s (id integer primary key,t character varying(100) NOT NULL,mv_id integer NOT NULL);", getTableName("d")));
    statement.execute(String.format("CREATE TABLE %s (id uuid primary key, t integer NOT NULL,geometry_full_geom TEXT,geometry_index TEXT);", getTableName("a3")));
    statement.execute(String.format("CREATE TABLE %s (a3_id uuid NOT NULL, a2_id uuid NOT NULL,t integer NOT NULL,seq integer NOT NULL);", getTableName("a3a")));
    statement.execute(String.format("CREATE TABLE %s ( m_id uuid NOT NULL,a_id text, m_key text NOT NULL,m_value text);", getTableName("m")));
    statement.execute(String.format("CREATE TABLE %s (id integer primary key, v character varying(30) NOT NULL);", getTableName("mv")));
    LOG.info("Created all the tables");

    connection.createStatement().execute(String.format("CREATE INDEX %s ON %s (t);", getTableName("a1_t_idx"), getTableName("a1")));
    connection.createStatement().execute(String.format("CREATE INDEX %s ON %s (f_id_1); ", getTableName("a1_f_id_1_idx"), getTableName("a1")));
    connection.createStatement().execute(String.format("CREATE INDEX %s ON %s (f_id_2);", getTableName("a1_f_id_2_idx"), getTableName("a1")));
    connection.createStatement().execute(String.format("CREATE INDEX %s ON %s (id);", getTableName("a1a_id_idx"), getTableName("a1a")));
    connection.createStatement().execute(String.format("CREATE INDEX %s ON %s (t);", getTableName("a1a_t_idx"), getTableName("a1a")));
    connection.createStatement().execute(String.format("CREATE INDEX %s ON %s (a2_id);", getTableName("a1a_a2_id_idx"), getTableName("a1a")));
    connection.createStatement().execute(String.format("CREATE INDEX %s ON %s (t);", getTableName("a2_t_idx"), getTableName("a2")));
    connection.createStatement().execute(String.format("CREATE INDEX %s ON %s (t);", getTableName("a3_t"), getTableName("a3")));
    connection.createStatement().execute(String.format("CREATE INDEX %s ON %s (t);", getTableName("a3a_t_idx"), getTableName("a3a")));
    connection.createStatement().execute(String.format("CREATE INDEX %s ON %s (a2_id);", getTableName("a3a_a2_id_idx"), getTableName("a3a")));
    connection.createStatement().execute(String.format("CREATE INDEX %s ON %s (a3_id);", getTableName("a3a_a3_id_idx"), getTableName("a3a")));
    connection.createStatement().execute(String.format("CREATE INDEX %s ON %s (t);", getTableName("d_t_idx"), getTableName("d")));
    connection.createStatement().execute(String.format("CREATE INDEX %s ON %s (mv_id);", getTableName("d_mv_idx"), getTableName("d")));
    connection.createStatement().execute(String.format("CREATE INDEX %s ON %s (m_id);", getTableName("m_idx"), getTableName("m")));
    LOG.info("Created all the indexes");
  }

  public long doRead() {
    return 1L;
  }

  long getNextLong(int threadIdx) {
    long a = nextLong.get(threadIdx);
    long b = nextLongMax.get(threadIdx);
    if (a > b) {
      a = atomicLong.getAndAdd(kAtomicDelta);
      b = a + kAtomicDelta - 1L;
      nextLongMax.put(threadIdx, b);
    }

    nextLong.put(threadIdx, a + 1L);
    return a;
  }

  int getNextInt(int threadIdx) {
    int a = nextInt.get(threadIdx);
    int b = nextIntMax.get(threadIdx);
    if (a > b) {
      a = atomicInt.getAndAdd(kAtomicDelta);
      b = a + kAtomicDelta - 1;
      nextIntMax.put(threadIdx, b);
    }

    nextInt.put(threadIdx, a + 1);
    return a;
  }

  UUID getNextUUID(int threadIdx) {
    long a = getNextLong(threadIdx);
    long b = getNextLong(threadIdx);
    return new UUID(a, b);
  }

  String getRandomString() {
    StringBuilder builder = new StringBuilder();
    int var2 = 7;

    while(var2-- != 0) {
      int character = (int)(Math.random() * (double)"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".length());
      builder.append("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".charAt(character));
    }

    return builder.toString();
  }

  private PreparedStatement getA1Statement(int threadIdx) throws Exception {
    PreparedStatement preparedStatement = preparedStatements.get(threadIdx).get(getTableName("a1"));

    for(int i = 0; i < rowsCount.get("a1"); ++i) {
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
    PreparedStatement preparedStatement = preparedStatements.get(threadIdx).get(getTableName("a1a"));

    for(int i = 0; i < rowsCount.get("a1a"); ++i) {
      preparedStatement.setObject(1, getNextUUID(threadIdx));
      preparedStatement.setObject(2, getNextUUID(threadIdx));
      preparedStatement.setInt(3, getNextInt(threadIdx));
      preparedStatement.setInt(4, getNextInt(threadIdx));
      preparedStatement.addBatch();
    }

    return preparedStatement;
  }

  private PreparedStatement getA2Statement(int threadIdx) throws Exception {
    PreparedStatement preparedStatement = preparedStatements.get(threadIdx).get(getTableName("a2"));
    Connection conn = connectionMap.get(threadIdx);

    for(int i = 0; i < rowsCount.get("a2"); ++i) {
      preparedStatement.setObject(1, getNextUUID(threadIdx));
      preparedStatement.setString(2, "\"name:de=Nowosibirsk\"");
      preparedStatement.setInt(3, getNextInt(threadIdx));

      for(int j = 4; j <= 10; ++j) {
        Integer[] arr = new Integer[0];
        Array sqlArr = conn.createArrayOf("int", arr);
        preparedStatement.setArray(j, sqlArr);
      }

      preparedStatement.addBatch();
    }

    return preparedStatement;
  }

  private PreparedStatement getDStatement(int threadIdx) throws Exception {
    PreparedStatement preparedStatement = preparedStatements.get(threadIdx).get(getTableName("d"));

    for(int i = 0; i < rowsCount.get("d"); ++i) {
      preparedStatement.setInt(1, getNextInt(threadIdx));
      preparedStatement.setString(2, getRandomString());
      preparedStatement.setInt(3, getNextInt(threadIdx));
      preparedStatement.addBatch();
    }

    return preparedStatement;
  }

  private PreparedStatement getA3Statement(int threadIdx) throws Exception {
    PreparedStatement preparedStatement = preparedStatements.get(threadIdx).get(getTableName("a3"));

    for(int i = 0; i < rowsCount.get("a3"); ++i) {
      preparedStatement.setObject(1, getNextUUID(threadIdx));
      preparedStatement.setInt(2, getNextInt(threadIdx));
      preparedStatement.setString(3, "");
      preparedStatement.setString(4, "0101000020E61000002A05DD5E525953C0F74773BF9EA54640");
      preparedStatement.addBatch();
    }

    return preparedStatement;
  }

  private PreparedStatement getA3AStatement(int threadIdx) throws Exception {
    PreparedStatement preparedStatement = preparedStatements.get(threadIdx).get(getTableName("a3a"));

    for(int i = 0; i < rowsCount.get("a3a"); ++i) {
      preparedStatement.setObject(1, getNextUUID(threadIdx));
      preparedStatement.setObject(2, getNextUUID(threadIdx));
      preparedStatement.setInt(3, getNextInt(threadIdx));
      preparedStatement.setInt(4, getNextInt(threadIdx));
      preparedStatement.addBatch();
    }

    return preparedStatement;
  }

  private PreparedStatement getMStatement(int threadIdx) throws Exception {
    PreparedStatement preparedStatement = preparedStatements.get(threadIdx).get(getTableName("m"));

    for(int i = 0; i < rowsCount.get("m"); ++i) {
      preparedStatement.setObject(1, getNextUUID(threadIdx));
      preparedStatement.setString(2, "abcdefghij");
      preparedStatement.setString(3, "abcdefghij");
      preparedStatement.setString(4, "abcdefghij");
      preparedStatement.addBatch();
    }

    return preparedStatement;
  }

  private PreparedStatement getMVStatement(int threadIdx) throws Exception {
    PreparedStatement preparedStatement = preparedStatements.get(threadIdx).get(getTableName("mv"));

    for(int i = 0; i < rowsCount.get("mv"); ++i) {
      preparedStatement.setInt(1, getNextInt(threadIdx));
      preparedStatement.setString(2, "abcdefghij");
      preparedStatement.addBatch();
    }

    return preparedStatement;
  }
  public long doWrite(int threadIdx) {
    try {
      PreparedStatement preparedStatement = getA1Statement(threadIdx);
      preparedStatement.executeBatch();
      preparedStatement = getA1AStatement(threadIdx);
      preparedStatement.executeBatch();
      preparedStatement = getA2Statement(threadIdx);
      preparedStatement.executeBatch();
      preparedStatement = getDStatement(threadIdx);
      preparedStatement.executeBatch();
      preparedStatement = getA3Statement(threadIdx);
      preparedStatement.executeBatch();
      preparedStatement = getA3AStatement(threadIdx);
      preparedStatement.executeBatch();
      preparedStatement = getMStatement(threadIdx);
      preparedStatement.executeBatch();
      preparedStatement = getMVStatement(threadIdx);
      preparedStatement.executeBatch();
      return 1;
    } catch (Exception e) {
      LOG.error("Error encountered while writing " + e);
      e.printStackTrace();
      return 0;
    }
  }

  public List<String> getWorkloadDescription() {
    return Arrays.asList("Sample key-value app built on PostgreSQL with concurrent readers and writers. The app inserts unique string keys", "each with a string value to a postgres table with an index on the value column.", "There are multiple readers and writers that update these keys and read them", "indefinitely, with the readers query the keys by the associated values that are", "indexed. Note that the number of reads and writes to perform can be specified as", "a parameter.");
  }

  public List<String> getWorkloadOptionalArguments() {
    return Arrays.asList("--num_unique_keys " + appConfig.numUniqueKeysToWrite, "--num_reads " + appConfig.numKeysToRead, "--num_writes " + appConfig.numKeysToWrite, "--num_threads_read " + appConfig.numReaderThreads, "--num_threads_write " + appConfig.numWriterThreads);
  }
}

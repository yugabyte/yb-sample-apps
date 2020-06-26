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

import com.datastax.driver.core.*;
import com.yugabyte.sample.common.SimpleLoadGenerator.Key;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * This workload writes and reads some random string keys from a YCQL table with a secondary index
 * on a non-primary-key column. When it reads a key, it queries the key by its associated value
 * which is indexed.
 */
public class CassandraInserts extends CassandraKeyValue {
  private static final Logger LOG = Logger.getLogger(CassandraSecondaryIndex.class);

  static {
    appConfig.readIOPSPercentage = -1;
    appConfig.numReaderThreads = 2;
    appConfig.numWriterThreads = 2;
    appConfig.numKeysToRead = -1;
    appConfig.numKeysToWrite = -1;
    appConfig.numUniqueKeysToWrite = NUM_UNIQUE_KEYS;
  }

  // The default table name to create and use for CRUD ops.
  private static final String DEFAULT_TABLE_NAME = CassandraInserts.class.getSimpleName();
  // The strings used for insert and select operations.
  private static String insertStatement;
  private static String selectStatement;

  // The number of value columns.
  private static int countValCols;

  public CassandraInserts() {
  }

  @Override
  public void initializeConnectionsAndStatements(int numThreads) {
    countValCols = appConfig.numIndexes > 0 ? appConfig.numIndexes : 1;

    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO " + getTableName());

    StringBuilder fields = new StringBuilder();
    StringBuilder values = new StringBuilder();

    fields.append("(k");
    values.append("(?");

    for (int i = 0; i < countValCols; ++i) {
      fields.append(", v" + i);
      values.append(", ?");
    }
    fields.append(")");
    values.append(")");

    sb.append(fields);
    sb.append(" VALUES ");
    sb.append(values);
    sb.append(";");
    insertStatement = sb.toString();
    selectStatement = String.format("SELECT * FROM %s WHERE k = ?;", getTableName());
  }

  @Override
  public void dropTable() {
    dropCassandraTable(getTableName());
  }

  @Override
  public List<String> getCreateTableStatements() {
    List<String> createStmts = new ArrayList<String>();

    StringBuilder create = new StringBuilder();
    create.append("CREATE TABLE IF NOT EXISTS ");
    create.append(getTableName());
    create.append("(k text,");

    for (int i = 0; i < countValCols; ++i) {
      create.append(" v");
      create.append(i);
      create.append(" text,");
    }
    create.append(" primary key (k)) ");
    if (!appConfig.nonTransactionalIndex) {
      create.append("WITH transactions = { 'enabled' : true };");
    }
    createStmts.add(create.toString());

    for (int i = 0; i < appConfig.numIndexes; ++i) {
      String index = String.format(
          "CREATE INDEX IF NOT EXISTS idx%d ON %s (v%d) %s;", i, getTableName(), i,
          appConfig.nonTransactionalIndex ?
              "WITH transactions = {'enabled' : false, 'consistency_level' : 'user_enforced'}" : "");
      createStmts.add(index);
    }
    return createStmts;
  }

  public String getTableName() {
    return appConfig.tableName != null ? appConfig.tableName : DEFAULT_TABLE_NAME;
  }

  private PreparedStatement getPreparedSelect() {
    return getPreparedSelect(selectStatement, appConfig.localReads);
  }

  @Override
  protected PreparedStatement getPreparedInsert() {
    return getPreparedInsert(insertStatement);
  }

  private static BoundStatement getBoundInsertStatement(PreparedStatement ps, Key key) {
    BoundStatement bs = ps.bind();
    bs.setString(0, key.asString());
    for (int i = 0; i < countValCols; ++i) {
      bs.setString(1 + i, key.getValueStr(i, appConfig.valueSize));
    }
    return bs;
  }

  @Override
  public long doRead() {
    Key key = getSimpleLoadGenerator().getKeyToRead();
    if (key == null) {
      return 0;
    }
    BoundStatement bound = getPreparedSelect().bind(key.asString());
    ResultSet rs = getCassandraClient().execute(bound);
    List<Row> rows = rs.all();
    LOG.debug("Read key: " + key.asString() + " return code" + rs.toString());
    if (rows.size() != 1) {
      LOG.fatal("Read key: " + key.asString() + " expected 1 row in result, got " + rows.size());
    }
    if (!key.asString().equals(rows.get(0).getString(0))) {
      LOG.fatal("Read key: " + key.asString() + ", got " + rows.get(0).getString(0));
    }
    return 1;
  }

  private int doBatchedWrite() {
    Set<Key> keys = new HashSet<Key>();
    try {
      BatchStatement batch = new BatchStatement();
      PreparedStatement insert = getPreparedInsert();
      for (int i = 0; i < appConfig.batchSize; i++) {
        Key key = getSimpleLoadGenerator().getKeyToWrite();
        keys.add(key);
        batch.add(getBoundInsertStatement(insert, key));
      }
      ResultSet resultSet = getCassandraClient().execute(batch);
      LOG.debug("Wrote keys count: " + keys.size() + ", return code: " + resultSet.toString());
      for (Key key : keys) {
        getSimpleLoadGenerator().recordWriteSuccess(key);
      }
      return keys.size();
    } catch (Exception e) {
      for (Key key : keys) {
        getSimpleLoadGenerator().recordWriteFailure(key);
      }
      throw e;
    }
  }

  private int doSingleWrite() {
    Key key = null;
    try {
      key = getSimpleLoadGenerator().getKeyToWrite();
      if (key == null) {
        return 0;
      }
      ResultSet resultSet =
          getCassandraClient().execute(getBoundInsertStatement(getPreparedInsert(), key));
      LOG.debug("Wrote key: " + key.toString() + ", return code: " + resultSet.toString());
      getSimpleLoadGenerator().recordWriteSuccess(key);
      return 1;
    } catch (Exception e) {
      getSimpleLoadGenerator().recordWriteFailure(key);
      throw e;
    }
  }

  @Override
  public long doWrite(int threadIdx) {
    if (appConfig.batchWrite) {
      return doBatchedWrite();
    }
    return doSingleWrite();
  }

  @Override
  public List<String> getWorkloadDescription() {
    return Arrays.asList(
        "Secondary index on key-value YCQL table. Writes unique keys with an index on values. ",
        " Supports a flag to run at app enforced consistency level which is ideal for batch ",
        "loading data.");
  }

  @Override
  public List<String> getWorkloadOptionalArguments() {
    return Arrays.asList(
        "--num_unique_keys " + appConfig.numUniqueKeysToWrite,
        "--num_reads " + appConfig.numKeysToRead,
        "--num_writes " + appConfig.numKeysToWrite,
        "--num_threads_read " + appConfig.numReaderThreads,
        "--num_threads_write " + appConfig.numWriterThreads,
        "--batch_size " + appConfig.batchSize,
        "--num_indexes" + appConfig.numIndexes,
        "--value_size " + appConfig.valueSize);
  }
}

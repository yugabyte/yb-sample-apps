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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import java.math.BigInteger;

import org.apache.log4j.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.yugabyte.sample.common.CmdLineOpts;

/**
 * This workload writes and reads some random string keys from a CQL server. By default, this app
 * inserts a million keys, and reads/updates them indefinitely.
 */
public class CassandraVarIntKeyValue extends CassandraAppBase {
  private static final Logger LOG = Logger.getLogger(CassandraVarIntKeyValue.class);

  // The default table name to create and use for CRUD ops.
  private static final String DEFAULT_TABLE_NAME = CassandraVarIntKeyValue.class.getSimpleName();

  private Random random = new Random();

  private static Object mutex = new Object();

  private static volatile BigInteger[] writtenKeys = new BigInteger[1024];

  private static volatile int writePosition = 0;

  @Override
  public List<String> getCreateTableStatements() {
    String create_stmt = String.format(
      "CREATE TABLE IF NOT EXISTS %s (h int, k varint, v varint, primary key ((h), k))",
      getTableName());

    if (appConfig.tableTTLSeconds > 0) {
      create_stmt += " WITH default_time_to_live = " + appConfig.tableTTLSeconds;
    }
    create_stmt += ";";
    return Arrays.asList(create_stmt);
  }

  @Override
  protected String getDefaultTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public long doRead() {
    BigInteger key;
    synchronized(mutex) {
      key = writtenKeys[random.nextInt(writtenKeys.length - 1)];
    }

    if (key == null) {
      try {
        Thread.sleep(100);
      } catch (Exception e) {
      }
      return 0;
    }

    try {
      // Do the write to Cassandra.
      PreparedStatement prepared_stmt = getPreparedSelect(
          String.format("SELECT k, v FROM %s WHERE h = ? AND k = ?;", getTableName()),
          appConfig.localReads);
      BoundStatement select = prepared_stmt.bind(key.hashCode(), key);
      ResultSet resultSet = getCassandraClient().execute(select);
      Row row = resultSet.one();
      BigInteger readKey = row.getVarint(0);
      BigInteger readValue = row.getVarint(1);

      if (!readKey.equals(key) || !readValue.equals(key.negate())) {
        LOG.fatal("Bad row for key " + key + ": " + readKey + ", " + readValue);
      }

      return 1;
    } catch (Exception e) {
      throw e;
    }
  }

  @Override
  public long doWrite(int threadIdx) {
    BigInteger key = new BigInteger(0x1000, random);

    try {
      // Do the write to Cassandra.
      BoundStatement insert = getPreparedInsert().bind(key.hashCode(), key, key.negate());
      ResultSet resultSet = getCassandraClient().execute(insert);
      LOG.debug("Wrote key: " + key.toString() + ", return code: " + resultSet.toString());

      synchronized(mutex) {
        writtenKeys[writePosition] = key;
        writePosition = (writePosition + 1) % writtenKeys.length;
      }

      return 1;
    } catch (Exception e) {
      throw e;
    }
  }

  protected PreparedStatement getPreparedInsert() {
    return getPreparedInsert(String.format(
        "INSERT INTO %s (h, k, v) VALUES (?, ?, ?);", getTableName()));
  }

  @Override
  public List<String> getWorkloadDescription() {
    return Arrays.asList(
      "Sample key-value app built on Cassandra. It uses VarInt as key and value.");
  }
}

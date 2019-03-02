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
public abstract class CassandraKeyValueBase extends CassandraAppBase {
  private static final Logger LOG = Logger.getLogger(CassandraKeyValueBase.class);

  protected abstract BoundStatement bindSelect(String key);

  @Override
  public long doRead() {
    SimpleLoadGenerator.Key key = getSimpleLoadGenerator().getKeyToRead();
    if (key == null) {
      // There are no keys to read yet.
      return 0;
    }
    // Do the read from Cassandra.
    // Bind the select statement.
    BoundStatement select = bindSelect(key.asString());
    ResultSet rs = getCassandraClient().execute(select);
    List<Row> rows = rs.all();
    if (rows.size() != 1) {
      // If TTL is enabled, turn off correctness validation.
      if (appConfig.tableTTLSeconds <= 0) {
        LOG.fatal("Read key: " + key.asString() + " expected 1 row in result, got " + rows.size());
      }
      return 1;
    }
    if (appConfig.valueSize == 0) {
      ByteBuffer buf = rows.get(0).getBytes(1);
      String value = new String(buf.array());
      key.verify(value);
    } else {
      ByteBuffer value = rows.get(0).getBytes(1);
      byte[] bytes = new byte[value.capacity()];
      value.get(bytes);
      verifyRandomValue(key, bytes);
    }
    LOG.debug("Read key: " + key.toString());
    return 1;
  }

  protected abstract BoundStatement bindInsert(String key, ByteBuffer value);

  @Override
  public long doWrite(int threadIdx) {
    SimpleLoadGenerator.Key key = getSimpleLoadGenerator().getKeyToWrite();
    if (key == null) {
      return 0;
    }

    try {
      // Do the write to Cassandra.
      BoundStatement insert;
      if (appConfig.valueSize == 0) {
        String value = key.getValueStr();
        insert = bindInsert(key.asString(), ByteBuffer.wrap(value.getBytes()));
      } else {
        byte[] value = getRandomValue(key);
        insert = bindInsert(key.asString(), ByteBuffer.wrap(value));
      }
      ResultSet resultSet = getCassandraClient().execute(insert);
      LOG.debug("Wrote key: " + key.toString() + ", return code: " + resultSet.toString());
      getSimpleLoadGenerator().recordWriteSuccess(key);
      return 1;
    } catch (Exception e) {
      getSimpleLoadGenerator().recordWriteFailure(key);
      throw e;
    }
  }
}

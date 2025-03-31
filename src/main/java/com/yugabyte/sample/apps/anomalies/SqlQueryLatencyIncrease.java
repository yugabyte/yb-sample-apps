package com.yugabyte.sample.apps.anomalies;

import com.yugabyte.sample.apps.SQLAppBase;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import org.apache.log4j.Logger;

public class SqlQueryLatencyIncrease extends SQLAppBase {

  private static final Logger LOG = Logger.getLogger(SqlQueryLatencyIncrease.class);

  private volatile String selectSeqScan;
  private volatile Connection selConnection = null;
  private volatile Statement statement = null;
  private volatile String selectIndex = null;
  private long seqScanStartTime = 0;
  private long seqScanEndTime = 0;

  private long lastReadLogTime = 0;

  private static final String DEFAULT_TABLE_NAME = "PostgresqlKeyValue";

  public SqlQueryLatencyIncrease() {
  }

  @Override
  public void dropTable() throws Exception {
    try (Connection connection = getPostgresConnection()) {
      connection.createStatement().execute("DROP TABLE IF EXISTS " + getTableName());
      LOG.info(String.format("Dropped table: %s", getTableName()));
    }
  }

  @Override
  public String getTableName() {
    String tableName = appConfig.tableName != null ? appConfig.tableName : DEFAULT_TABLE_NAME;
    return tableName.toLowerCase();
  }

  @Override
  public void createTablesIfNeeded(TableOp tableOp) throws Exception {
    try (Connection connection = getPostgresConnection()) {
      Statement s = connection.createStatement();
      s.addBatch(
          String.format(
              "CREATE TABLE IF NOT EXISTS %s (k int PRIMARY KEY, v1 int, v2 int) SPLIT INTO 3 TABLETS;",
              getTableName()));

      s.addBatch(
          String.format(
              "CREATE INDEX IF NOT EXISTS %s_v1_v2 ON %s(v1,v2);", getTableName(), getTableName()));
      s.executeBatch();
      LOG.info("Created table and index " + getTableName());

      if (tableOp.equals(TableOp.TruncateTable)) {
        Statement t = connection.createStatement();
        t.execute(String.format("TRUNCATE TABLE %s;", getTableName()));
        LOG.info("Truncated table " + getTableName());
      }

      s = connection.createStatement();
      long rowsToInsert = appConfig.numKeysToWrite;
      long maxValue = Math.min(rowsToInsert / 100, 100);
      String createIndexStatement =
          String.format(
              "INSERT INTO %s SELECT i, i / " + maxValue + ", i %% " + maxValue + " FROM"
                  + " generate_series(1, " + rowsToInsert + ") AS i WHERE (SELECT COUNT(*) from %s) = 0;",
              getTableName(), getTableName());
      LOG.info("Inserting data: " + createIndexStatement);
      s.execute(createIndexStatement);

      LOG.info("Inserted " + rowsToInsert + " rows into table " + getTableName());

      s = connection.createStatement();
      s.execute(String.format("ANALYZE %s;", getTableName()));
      LOG.info("Analyze. Executing");
    }
  }

  private String getSelect() throws Exception {
    if (selectIndex == null || selectSeqScan == null) {
      LOG.info("Preparing SELECT statements");
      close(selConnection);
      selConnection = getPostgresConnection();
      statement = selConnection.createStatement();

      String query = String.format("select * from %s where v1 = :v1 and v2 = :v2;", getTableName());
      String indexHint = String.format("/*+IndexScan(%s %s_v1_v2)*/", getTableName(), getTableName());
      String seqScan = String.format("/*+SeqScan(%s)*/", getTableName());

      LOG.info("selIndex " + indexHint + query);
      LOG.info("selSeqScan " + seqScan + query);

      selectIndex = indexHint + query;
      selectSeqScan = seqScan + query;

      setupSkew();
    }

    String select = selectIndex;
    long currentTimeSec = (System.currentTimeMillis() - workloadStartTime) / 1000;
    if (currentTimeSec > seqScanStartTime && currentTimeSec < seqScanEndTime) {
      if (System.currentTimeMillis() - 3000 > lastReadLogTime) {
        // Let's notify once per 3 seconds
        LOG.info(
            "Read Skew Iteration: " + (currentTimeSec - seqScanStartTime)
                + " / " + (seqScanEndTime - seqScanStartTime) + " sec done.");
        lastReadLogTime = System.currentTimeMillis();
      }
      select = selectSeqScan;
    }
    return select;
  }

  protected void setupSkew() {
    boolean timeBased = appConfig.runTimeSeconds > 0;

    if (timeBased) {
      // 6/8 normal traffic + 1/8 high latency traffic + 1/8 normal traffic
      seqScanStartTime = appConfig.runTimeSeconds / 8 * 6;
      seqScanEndTime = appConfig.runTimeSeconds / 8 * 7;

      LOG.info(
          "Setting up read skew appConfig.runTimeSeconds = "
              + appConfig.runTimeSeconds
              + " configuration.getNumReaderThreads() = "
              + configuration.getNumReaderThreads()
              + " seqScanStartTime = "
              + seqScanStartTime
              + " seqScanEndTime = "
              + seqScanEndTime);
    } else {
      throw new IllegalArgumentException("This workload only support time based run."
          + " Please specify --run_time parameter");
    }
  }

  @Override
  public long doRead() {
    int rowsToInsert = (int) appConfig.numKeysToWrite;
    long maxValue = Math.min(rowsToInsert / 100, 100);
    long key = random.nextInt(rowsToInsert);
    long v1 = key / maxValue;
    long v2 = key % maxValue;
    try {
      String select = getSelect();
      statement.execute(select
            .replace(":v1", String.valueOf(v1))
            .replace(":v2", String.valueOf(v2)));
    } catch (Exception e) {
      LOG.info("Failed reading value " + v1 + ":" + v2, e);
      if (statement != null) {
        try {
          statement.close();
        } catch (Throwable t) {
          LOG.warn("encountered exception closing statement: " + t);
        }
      }
      statement = null;
      selectSeqScan = null;
      selectIndex = null;
      return 0;
    }
    return 1;
  }
}

package com.yugabyte.sample.apps.anomalies;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;
import org.apache.log4j.Logger;

public class PlanAnomalyV2 extends PlanAnomaly {

  private static final Logger LOG = Logger.getLogger(PlanAnomalyV2.class);
  private static final int KEY_LIMIT = 1000000;

  public PlanAnomalyV2() {
    buffer = new byte[appConfig.valueSize];
    WAIT_TIMEOUT_MS = 3000;
    // We start at 1/4 of the queries
    beginRatio = 4;
    // The ratio between the queries is ~20x
    // Without hint: 45ms
    // With hint: 2ms
    percentage = 30;
  }

  @Override
  public void createTablesIfNeeded(TableOp tableOp) throws Exception {
    try (Connection connection = getPostgresConnection()) {

      if (tableOp.equals(TableOp.DropTable)) {
        LOG.info("Dropping any table(s) left from previous runs if any");
        Statement s = connection.createStatement();
        s.addBatch(String.format("DROP TABLE IF EXISTS %s CASCADE;", getTableName()));
        s.executeBatch();
        LOG.info("Dropped");
      }

      Statement s = connection.createStatement();
      s.addBatch(
          String.format(
              "CREATE TABLE IF NOT EXISTS %s (k1 int, k2 int, v1 int, v2 int, v3 int, PRIMARY"
                  + " KEY(k1,k2)) SPLIT INTO 3 TABLETS;",
              getTableName()));

      s.addBatch(
          String.format(
              "CREATE INDEX IF NOT EXISTS %s_v1_v2 ON %s(v1,v2);", getTableName(), getTableName()));
      s.executeBatch();
      LOG.info("Created table and index " + getTableName());

      if (tableOp.equals(TableOp.TruncateTable)) {
        Statement t = connection.createStatement();
        t.addBatch(String.format("TRUNCATE TABLE %s;", getTableName()));
        t.executeBatch();
        LOG.info("Truncated table " + getTableName());
      }

      s = connection.createStatement();
      String createIndexStatement =
          String.format(
              "INSERT INTO %s SELECT i / 100000,i %% 100000,i / 10,i %% 10, i FROM"
                  + " generate_series(1,1000000) AS i WHERE (SELECT COUNT(*) from %s) = 0;",
              getTableName(), getTableName());
      LOG.info("Create index: " + createIndexStatement);
      s.addBatch(createIndexStatement);
      s.executeBatch();

      LOG.info("Inserted table " + getTableName());

      s = connection.createStatement();
      s.addBatch(String.format("ANALYZE %s;", getTableName()));
      LOG.info("Analyze. Executing");
      s.executeBatch();
    }
  }

  @Override
  public PreparedStatement getPreparedSelect() throws Exception {
    if (preparedSelect == null) {
      LOG.info("Preparing SELECT statement");
      close(selConnection);
      selConnection = getPostgresConnectionFair();

      String query = String.format("select * from %s where v1 = ? and k1 = ?;", getTableName());
      String hint = String.format("/*+IndexScan(%s %s_v1_v2)*/", getTableName(), getTableName());

      LOG.info("sel " + hint + query);
      LOG.info("selNoHint " + query);

      preparedSelect = selConnection.prepareStatement(hint + query);
      preparedSelectNoHint = selConnection.prepareStatement(query);
      LOG.info("Prepared SELECT statement");
    }

    return preparedSelect;
  }

  @Override
  public void executeQuery(PreparedStatement statement) throws Exception {
    long key = getNextKey();
    statement.setLong(1, key / 10);
    statement.setLong(2, key / 100000);
    try (ResultSet rs1 = statement.executeQuery()) {}
  }

  public long getNextKey() {
    return random.nextInt(KEY_LIMIT);
  }
}

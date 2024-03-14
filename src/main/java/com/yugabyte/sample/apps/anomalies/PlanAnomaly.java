package com.yugabyte.sample.apps.anomalies;

import com.yugabyte.sample.common.CmdLineOpts.ContactPoint;
import com.yugabyte.sample.common.SimpleLoadGenerator.Key;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;

public class PlanAnomaly extends SqlInsertTablets {

  private static final Logger LOG = Logger.getLogger(PlanAnomaly.class);

  public PreparedStatement preparedSelectNoHint;

  private static AtomicInteger readSkewSetup = new AtomicInteger(0);
  private boolean readSkewThread = false;
  public int readCounter = 0;
  public long lowerBound = 0;
  public long upperBound = 0;

  public PlanAnomaly() {
    buffer = new byte[appConfig.valueSize];
    WAIT_TIMEOUT_MS = 3000;
  }

  public void setupHinting() throws ClassNotFoundException, SQLException {
    String dbName = appConfig.defaultPostgresDatabase;
    String dbUser = appConfig.dbUsername;
    String dbPass = appConfig.dbPassword;
    appConfig.enableDriverDebug = true;

    for (ContactPoint contactPoint : configuration.contactPoints) {
      LOG.info("Setting up pg_hint_plan extension for host " + contactPoint.getHost());
      Connection connection =
          getRawConnection(contactPoint.getHost(), contactPoint.getPort(), dbName, dbUser, dbPass);
      Statement s = connection.createStatement();
      s.addBatch("CREATE EXTENSION IF NOT EXISTS pg_hint_plan;");
      s.addBatch("SET pg_hint_plan.enable_hint_table = on;");
      s.addBatch("truncate hint_plan.hints;");
      s.addBatch(
          "INSERT INTO hint_plan.hints(norm_query_string, application_name, hints)VALUES('SELECT"
              + " DISTINCT t50000.c_int,t500000.c_varchar FROM   t500000 right OUTER JOIN t100 ON"
              + " t500000.c_decimal = t100.c_decimal right OUTER JOIN t50000 ON t500000.c_decimal ="
              + " t50000.c_decimal WHERE  t500000.c_int in (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
              + " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
              + " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ORDER BY t50000.c_int, t500000.c_varchar desc limit"
              + " ?;', '', 'Leading ( t50000 t500000 ) t100 HashJoin(t50000 t500000)"
              + " HashJoin(t50000 t500000 t100) IndexOnlyScan(t500000) SeqScan(t100)"
              + " IndexScan(t50000)');");
      s.executeBatch();
      close(connection);
    }
  }

  @Override
  public void createTablesIfNeeded(TableOp tableOp) throws Exception {
    setupHinting();
    try (Connection connection = getPostgresConnection()) {

      if (tableOp.equals(TableOp.DropTable)) {

        LOG.info("Dropping any table(s) left from previous runs if any");
        Statement s = connection.createStatement();
        s.addBatch("DROP TABLE IF EXISTS t1000000 CASCADE;");
        s.addBatch("DROP TABLE IF EXISTS t500000 CASCADE;");
        s.addBatch("DROP TABLE IF EXISTS t50000 CASCADE;");
        s.addBatch("DROP TABLE IF EXISTS t100 CASCADE;");
        s.executeBatch();
        LOG.info("Dropped");
      }

      Statement s = connection.createStatement();
      s.addBatch(
          "CREATE TABLE IF NOT EXISTS t1000000\n"
              + "AS\n"
              + "SELECT c_int,\n"
              + "       (case when c_int % 2 = 0 then true else false end) as c_bool,\n"
              + "       (c_int + 0.0001)::text as c_text,\n"
              + "        (c_int + 0.0002):: varchar as c_varchar,\n"
              + "        (c_int + 0.1):: decimal as c_decimal,\n"
              + "        (c_int + 0.2):: float as c_float,\n"
              + "        (c_int + 0.3):: real as c_real,\n"
              + "        (c_int + 0.4) ::money as c_money FROM generate_Series(1, 100000 * 10)"
              + " c_int;");
      s.addBatch("CREATE INDEX IF NOT EXISTS t1000000_1_idx ON t1000000 (c_int ASC);");
      s.addBatch("CREATE INDEX IF NOT EXISTS t1000000_2_idx ON t1000000 (c_int ASC, c_bool ASC);");
      s.addBatch("CREATE INDEX IF NOT EXISTS t1000000_3_idx ON t1000000 (c_int ASC, c_text ASC);");
      s.addBatch(
          "CREATE INDEX IF NOT EXISTS t1000000_4_idx ON t1000000 (c_int ASC, c_varchar ASC);");
      s.addBatch(
          "CREATE INDEX IF NOT EXISTS t1000000_5_idx ON t1000000 (c_float ASC, c_text ASC,"
              + " c_varchar ASC);");
      s.addBatch(
          "CREATE INDEX IF NOT EXISTS t1000000_6_idx ON t1000000 (c_float ASC, c_decimal ASC,"
              + " c_varchar ASC);");
      s.addBatch(
          "CREATE INDEX IF NOT EXISTS t1000000_7_idx ON t1000000 (c_float ASC, c_real ASC, c_money"
              + " ASC);");
      LOG.info("t1000000");

      s.addBatch(
          "CREATE TABLE IF NOT EXISTS t500000\n"
              + "AS\n"
              + "SELECT c_int,\n"
              + "       (case when c_int % 2 = 0 then true else false end) as c_bool,\n"
              + "       (c_int + 0.0001)::text as c_text,\n"
              + "        (c_int + 0.0002):: varchar as c_varchar,\n"
              + "        (c_int + 0.1):: decimal as c_decimal,\n"
              + "        (c_int + 0.2):: float as c_float,\n"
              + "        (c_int + 0.3):: real as c_real,\n"
              + "        (c_int + 0.4) ::money as c_money FROM generate_Series(1, 50000 * 10)"
              + " c_int;");
      s.addBatch("CREATE INDEX IF NOT EXISTS t500000_1_idx ON t500000 (c_int ASC);");
      s.addBatch("CREATE INDEX IF NOT EXISTS t500000_2_idx ON t500000 (c_int ASC, c_bool ASC);");
      s.addBatch("CREATE INDEX IF NOT EXISTS t500000_3_idx ON t500000 (c_int ASC, c_text ASC);");
      s.addBatch("CREATE INDEX IF NOT EXISTS t500000_4_idx ON t500000 (c_int ASC, c_varchar ASC);");
      s.addBatch(
          "CREATE INDEX IF NOT EXISTS t500000_5_idx ON t500000 (c_float ASC, c_text ASC, c_varchar"
              + " ASC);");
      s.addBatch(
          "CREATE INDEX IF NOT EXISTS t500000_6_idx ON t500000 (c_float ASC, c_decimal ASC,"
              + " c_varchar ASC);");
      s.addBatch(
          "CREATE INDEX IF NOT EXISTS t500000_7_idx ON t500000 (c_float ASC, c_real ASC, c_money"
              + " ASC);");
      LOG.info("t500000");

      s.addBatch(
          "CREATE TABLE IF NOT EXISTS t50000\n"
              + "AS\n"
              + "SELECT c_int,\n"
              + "       (case when c_int % 2 = 0 then true else false end) as c_bool,\n"
              + "       (c_int + 0.0001)::text as c_text,\n"
              + "        (c_int + 0.0002):: varchar as c_varchar,\n"
              + "        (c_int + 0.1):: decimal as c_decimal,\n"
              + "        (c_int + 0.2):: float as c_float,\n"
              + "        (c_int + 0.3):: real as c_real,\n"
              + "        (c_int + 0.4) ::money as c_money FROM generate_Series(1, 5000 * 10)"
              + " c_int;");
      s.addBatch("CREATE INDEX IF NOT EXISTS t50000_1_idx ON t50000 (c_int ASC);");
      s.addBatch("CREATE INDEX IF NOT EXISTS t50000_2_idx ON t50000 (c_int ASC, c_bool ASC);");
      s.addBatch("CREATE INDEX IF NOT EXISTS t50000_3_idx ON t50000 (c_int ASC, c_text ASC);");
      s.addBatch("CREATE INDEX IF NOT EXISTS t50000_4_idx ON t50000 (c_int ASC, c_varchar ASC);");
      s.addBatch(
          "CREATE INDEX IF NOT EXISTS t50000_5_idx ON t50000 (c_float ASC, c_text ASC, c_varchar"
              + " ASC);");
      s.addBatch(
          "CREATE INDEX IF NOT EXISTS t50000_6_idx ON t50000 (c_float ASC, c_decimal ASC, c_varchar"
              + " ASC);");
      s.addBatch(
          "CREATE INDEX IF NOT EXISTS t50000_7_idx ON t50000 (c_float ASC, c_real ASC, c_money"
              + " ASC);");
      LOG.info("t50000");

      s.addBatch(
          "CREATE TABLE IF NOT EXISTS t100\n"
              + "AS\n"
              + "SELECT c_int,\n"
              + "       (case when c_int % 2 = 0 then true else false end) as c_bool,\n"
              + "       (c_int + 0.0001)::text as c_text,\n"
              + "        (c_int + 0.0002):: varchar as c_varchar,\n"
              + "        (c_int + 0.1):: decimal as c_decimal,\n"
              + "        (c_int + 0.2):: float as c_float,\n"
              + "        (c_int + 0.3):: real as c_real,\n"
              + "        (c_int + 0.4) ::money as c_money FROM generate_Series(1, 10 * 10)"
              + " c_int;");
      s.addBatch("CREATE INDEX IF NOT EXISTS t100_1_idx ON t100 (c_int ASC);");
      s.addBatch("CREATE INDEX IF NOT EXISTS t100_2_idx ON t100 (c_int ASC, c_bool ASC);");
      s.addBatch("CREATE INDEX IF NOT EXISTS t100_3_idx ON t100 (c_int ASC, c_text ASC);");
      s.addBatch("CREATE INDEX IF NOT EXISTS t100_4_idx ON t100 (c_int ASC, c_varchar ASC);");
      s.addBatch(
          "CREATE INDEX IF NOT EXISTS t100_5_idx ON t100 (c_float ASC, c_text ASC, c_varchar"
              + " ASC);");
      s.addBatch(
          "CREATE INDEX IF NOT EXISTS t100_6_idx ON t100 (c_float ASC, c_decimal ASC, c_varchar"
              + " ASC);");
      s.addBatch(
          "CREATE INDEX IF NOT EXISTS t100_7_idx ON t100 (c_float ASC, c_real ASC, c_money ASC);");
      LOG.info("t100");

      s.addBatch("ANALYZE t1000000;");
      s.addBatch("ANALYZE t500000;");
      s.addBatch("ANALYZE t50000;");
      s.addBatch("ANALYZE t100;");
      LOG.info("Analyze. Executing");

      s.executeBatch();
      LOG.info("Tables created");

      if (tableOp.equals(TableOp.TruncateTable)) {
        Statement t = connection.createStatement();
        t.addBatch("TRUNCATE TABLE t1000000;");
        t.addBatch("TRUNCATE TABLE t500000;");
        t.addBatch("TRUNCATE TABLE t50000;");
        t.addBatch("TRUNCATE TABLE t100;");
        t.executeBatch();
        LOG.info("Truncated table: t1000000, t500000, t50000, t100");
      }
    }
  }

  @Override
  public PreparedStatement getPreparedSelect() throws Exception {
    if (preparedSelect == null) {
      LOG.info("Preparing SELECT statement");
      close(selConnection);
      selConnection = getPostgresConnectionFair();
      selConnection.createStatement().execute("SET pg_hint_plan.enable_hint_table = on;");
      selConnection.createStatement().execute("SET pg_hint_plan.debug_print TO on;");
      selConnection.createStatement().execute("SET client_min_messages TO log;");

      preparedSelect =
          selConnection.prepareStatement(
              "/*+Leading ( t50000 t500000 ) t100 HashJoin(t50000 t500000) HashJoin(t50000 t500000"
                  + " t100) IndexOnlyScan(t500000) SeqScan(t100) IndexScan(t50000)*/SELECT DISTINCT"
                  + " t50000.c_int,t500000.c_varchar FROM   t500000 right OUTER JOIN t100 ON"
                  + " t500000.c_decimal = t100.c_decimal right OUTER JOIN t50000 ON"
                  + " t500000.c_decimal = t50000.c_decimal WHERE  t500000.c_int in (13, 17, 74, 93,"
                  + " 76, 8, 82, 44, 26, 40, 96, 42, 3, 38, 98, 60, 1, 81, 62, 6, 1, 63, 29, 62,"
                  + " 93, 81, 35, 20, 28, 61, 56, 67, 8, 9, 62, 15, 51, 62, 81, 70, 40, 58, 95, 34,"
                  + " 74, 36, 80, 9, 74, 18) ORDER BY t50000.c_int, t500000.c_varchar desc limit"
                  + " 100;");
      preparedSelectNoHint =
          selConnection.prepareStatement(
              "SELECT DISTINCT t50000.c_int,t500000.c_varchar FROM   t500000 right OUTER JOIN t100"
                  + " ON t500000.c_decimal = t100.c_decimal right OUTER JOIN t50000 ON"
                  + " t500000.c_decimal = t50000.c_decimal WHERE  t500000.c_int in (13, 17, 74, 93,"
                  + " 76, 8, 82, 44, 26, 40, 96, 42, 3, 38, 98, 60, 1, 81, 62, 6, 1, 63, 29, 62,"
                  + " 93, 81, 35, 20, 28, 61, 56, 67, 8, 9, 62, 15, 51, 62, 81, 70, 40, 58, 95, 34,"
                  + " 74, 36, 80, 9, 74, 18) ORDER BY t50000.c_int, t500000.c_varchar desc limit"
                  + " 100;");

      // selConnection.createStatement().execute("SELECT pg_stat_statements_reset();");
      preparedSelectNoHint.execute();
      LOG.info("Prepared SELECT statement");
    }
    return preparedSelect;
  }

  @Override
  public long doRead() {
    try {
      doReadNoBarrier(null);
      return 1;
    } catch (Exception e) {
      LOG.info("Failed reading value: ", e);
      close(preparedSelect);
      preparedSelect = null;
      return 0;
    }
  }

  @Override
  public long doReadNoBarrier(Key key) {
    ResultSet rs;
    ResultSetMetaData rsmd;
    int columnsNumber;
    try {

      if (readSkewSetup.get() < 10) {
        if (readSkewSetup.incrementAndGet() == 1) {
          LOG.info("Setting up read skew thread");
          readSkewThread = true;
          lowerBound = appConfig.numKeysToRead / configuration.getNumReaderThreads() / 3;
          upperBound = lowerBound * 2;
        }
      }

      PreparedStatement statement;
      if (readSkewThread && readCounter > lowerBound && readCounter < upperBound) {
        if (readCounter % 100 == 0) {
          LOG.info("Read Skew Iteration: " + readCounter + " / " + upperBound + " done.");
        }
        statement = preparedSelectNoHint;
      } else {
        statement = getPreparedSelect();
      }

      try (ResultSet rs1 = statement.executeQuery()) {}
      readCounter++;

    } catch (Exception e) {
      LOG.info("Failed reading value: ", e);
      close(preparedSelect);
      preparedSelect = null;
      return 0;
    }
    return 1;
  }

  @Override
  public void recordExistingRowCount() throws Exception {
    LOG.info("Skipping recording the row count");
  }
}

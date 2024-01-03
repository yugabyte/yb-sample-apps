package com.yugabyte.sample.apps;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class SQLAppBase extends AppBase {

    private Logger LOG = Logger.getLogger(SQLAppBase.class);

    private long initialRowCount = 0;

    protected long getRowCount() throws Exception {
        long count = 0;
        String table = getTableName();
        Connection connection = getPostgresConnection();
        try {
            Statement statement = connection.createStatement();
            String query = "SELECT COUNT(*) FROM " + table;
            ResultSet rs = statement.executeQuery(query);
            if (rs.next()) {
                count = rs.getLong(1);
                LOG.info("Row count in table " + table + ": " + count);
            } else {
                LOG.error("No result received!");
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
        return count;
    }

    @Override
    public void verifyTotalRowsWritten() throws Exception {
        if (appConfig.numKeysToWrite >= 0) {
            String table = getTableName();
            LOG.info("Verifying the inserts on table " + table + " (" + initialRowCount + " rows initially) ...");
            LOG.info("appConfig.numKeysToWrite: " + appConfig.numKeysToWrite + ", numKeysWritten: " + numKeysWritten);
            long actual = getRowCount();
            long expected = numKeysWritten.get();
            if (actual != (expected + initialRowCount)) {
                LOG.fatal("New rows count does not match! Expected: " + (expected + initialRowCount) + ", actual: " + actual);
            } else {
                LOG.info("Table row count verified successfully");
            }
        }
    }

    @Override
    public void recordExistingRowCount() throws Exception {
        LOG.info("Recording the row count in table " + getTableName() + " if it exists ...");
        initialRowCount = getRowCount();
    }

}

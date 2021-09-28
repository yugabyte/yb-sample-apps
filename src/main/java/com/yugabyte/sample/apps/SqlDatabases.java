package com.yugabyte.sample.apps;

import com.yugabyte.sample.common.SimpleLoadGenerator;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class SqlDatabases extends AppBase {
    private static final Logger LOG = Logger.getLogger(SqlInserts.class);

    static {
        // Set the read and write threads to 2 each.
        appConfig.numWriterThreads = 8;
        // The number of keys to write. This is the combined total number of inserts and updates.
        appConfig.numKeysToWrite = 1000;
    }

    // The default table name to create and use for CRUD ops.
    private static final String DEFAULT_DATABASE_PREFIX = "database";

    private static final AtomicLong databaseNameCounter = new AtomicLong();

    @Override
    public long doWrite(int threadIdx) {
        int result = 0;
        String dbName = "EMPTY";
        try {
            if (ThreadLocalRandom.current().nextBoolean()) {
                long dbNameIndex = databaseNameCounter.incrementAndGet();
                dbName = DEFAULT_DATABASE_PREFIX + "_" + dbNameIndex;
                StringBuilder stmtString = new StringBuilder("CREATE DATABASE ")
                        .append(dbName);
                if (ThreadLocalRandom.current().nextBoolean()) {
                    stmtString.append(" WITH ENCODING 'utf8' ");
                } else if (ThreadLocalRandom.current().nextBoolean()) {
                    stmtString.append(" TEMPLATE template0;");
                }

                Connection connection = getPostgresConnection();
                result = connection.createStatement().executeUpdate(stmtString.toString());
                connection.createStatement().executeUpdate("CREATE TABLE T1 (a INT)");
                connection.createStatement().executeUpdate("CREATE TABLE T2 (a INT)");


                LOG.debug("Created database and table: " + DEFAULT_DATABASE_PREFIX + "_" + dbName + ", Result: " + result);

                getSimpleLoadGenerator().recordWriteSuccess(new SimpleLoadGenerator.Key(dbNameIndex, "APPEND"));
            } else {
                if (databaseNameCounter.get() != 0) {
                    long toRemove = ThreadLocalRandom.current().nextLong(0, databaseNameCounter.get());
                    dbName = DEFAULT_DATABASE_PREFIX + "_" + toRemove;

                    String stmtString = "DROP DATABASE IF EXISTS " + dbName;

                    result = getPostgresConnection().createStatement().executeUpdate(stmtString);

                    LOG.debug("Created database: " + DEFAULT_DATABASE_PREFIX + "_" + toRemove + ", Result: " + result);
                    getSimpleLoadGenerator().recordWriteSuccess(new SimpleLoadGenerator.Key(toRemove, "REMOVE"));
                }
            }
        } catch (Exception e) {
            LOG.info("Failed creating database " + dbName, e);
        }

        return result;
    }

    @Override
    public List<String> getWorkloadDescription() {
        return Arrays.asList("Creating and removing databases in parallel. Inspired by SQLancer tests.");
    }
}

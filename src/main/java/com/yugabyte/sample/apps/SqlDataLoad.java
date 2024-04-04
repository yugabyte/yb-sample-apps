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

import java.sql.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;

import com.yugabyte.sample.apps.AppBase.TableOp;
import com.yugabyte.sample.common.SimpleLoadGenerator.Key;

/**
 * This workload sets up from a YSQL table with user-given number of secondary indexes and/or
 * foreign key constraints.
 * Then runs a write workload to load data into the target table.
 */
public class SqlDataLoad extends AppBase {
    private static final Logger LOG = Logger.getLogger(SqlInserts.class);

    // Static initialization of this workload's config. These are good defaults for getting a decent
    // read dominated workload on a reasonably powered machine. Exact IOPS will of course vary
    // depending on the machine and what resources it has to spare.
    static {
        // Disable the read-write percentage.
        appConfig.readIOPSPercentage = -1;
        // Disable reads for this workload.
        appConfig.numReaderThreads = 0;

        // Set the write thread to 2 by default.
        appConfig.numWriterThreads = 2;
        // The number of keys to read.
        appConfig.numKeysToRead = -1;
        // The number of keys to write.
        appConfig.numKeysToWrite = -1;
        // When to switch from inserts to updates -- in this workload never by default.
        appConfig.numUniqueKeysToWrite = -1;
    }

    // The default table name to create and use for CRUD ops.
    private static final String DEFAULT_TABLE_NAME = "SqlDataLoad";

    // The prepared select statement for fetching the data.
    private volatile Connection selConnection = null;
    private PreparedStatement preparedSelect = null;

    // The prepared insert statement for inserting the data.
    private volatile Connection insConnection = null;
    private PreparedStatement preparedInsert = null;

    // Lock for initializing prepared statement objects.
    private static final Object prepareInitLock = new Object();

    public SqlDataLoad() {
        buffer = new byte[appConfig.valueSize];
    }

    /**
     * Drop the table created by this app.
     */
    @Override
    public void dropTable() throws Exception {
      try (Connection connection = getPostgresConnection()) {
        connection.createStatement().execute("DROP TABLE IF EXISTS " + getTableName());
        LOG.info(String.format("Dropped table: %s", getTableName()));
        for (int i = 1; i <= appConfig.numForeignKeys; i++) {
            String fkName = getFkTableName(i);
            connection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s CASCADE", fkName));
        }
      }
    }

    private String getFkTableName(int idx) {
        return String.format("%s_fk_%d", getTableName(), idx);
    }

    @Override
    public void createTablesIfNeeded(TableOp tableOp) throws Exception {
        try (Connection connection = getPostgresConnection();
             Statement statement = connection.createStatement()) {

            statement.execute(
                    String.format("DROP TABLE IF EXISTS %s CASCADE", getTableName()));
            LOG.info("Dropped table(s) left from previous runs if any");

            // Initialize foreign key tables (if any) -- create tables and load sample data.
            for (int i = 1; i <= appConfig.numForeignKeys; i++) {
                String fkName = getFkTableName(i);

                statement.execute(String.format("DROP TABLE IF EXISTS %s CASCADE", fkName));

                statement.execute(String.format("CREATE TABLE %s (k varchar PRIMARY KEY)", fkName));

                String fkInsert = String.format("INSERT INTO %s (k) VALUES (?)", fkName);
                PreparedStatement preparedFkInsert = connection.prepareStatement(fkInsert);

                for (int j = 1; j <= appConfig.numForeignKeyTableRows; j++) {
                    preparedFkInsert.setString(1, String.valueOf(j));
                    preparedFkInsert.executeUpdate();
                }
            }

            if (appConfig.numForeignKeys > 0) {
                LOG.info(String.format("Initialized %d foreign-key tables",
                                       appConfig.numForeignKeys));
            }

            // Create main table.
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TABLE ");
            sb.append(getTableName());
            sb.append(" (k varchar PRIMARY KEY");

            for (int i = 1; i <= appConfig.numValueColumns; i++) {
                String cname = "v" + i;
                sb.append(", ").append(cname).append(" varchar");

                if (i <= appConfig.numForeignKeys) {
                    sb.append(" REFERENCES ").append(getFkTableName(i));
                }

            }

            sb.append(")");
            statement.execute(sb.toString());
            LOG.info(String.format("Created table: %s", getTableName()));

            // Create secondary indexes (if any).
            for (int i = 0; i < appConfig.numIndexes; i++) {
                int colidx = i % appConfig.numValueColumns + 1;
                String cname = "v" + colidx;
                String idx_stmt = String.format("CREATE INDEX %sByValue%s_idx%d ON %s (%s)",
                                                getTableName(),
                                                cname,
                                                i,
                                                getTableName(),
                                                cname);
                statement.execute(idx_stmt);
            }

            if (appConfig.numIndexes > 0) {
                LOG.info(String.format("Created %d secondary indexes",
                                       appConfig.numIndexes));
            }
        }
    }

    public String getTableName() {
        String tableName = appConfig.tableName != null ? appConfig.tableName : DEFAULT_TABLE_NAME;
        return tableName.toLowerCase();
    }

    private PreparedStatement getPreparedSelect() throws Exception {
        if (preparedSelect == null) {
            close(selConnection);
            selConnection = getPostgresConnection();
            preparedSelect = selConnection.prepareStatement(
                    String.format("SELECT k, v FROM %s WHERE k = ?;", getTableName()));
        }
        return preparedSelect;
    }

    @Override
    public long doRead() {
        Key key = getSimpleLoadGenerator().getKeyToRead();
        if (key == null) {
            // There are no keys to read yet.
            return 0;
        }

        try {
            PreparedStatement statement = getPreparedSelect();
            statement.setString(1, key.asString());
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    LOG.error("Read key: " + key.asString() + " expected 1 row in result, got 0");
                    return 0;
                }

                if (!key.asString().equals(rs.getString("k"))) {
                    LOG.error("Read key: " + key.asString() + ", got " + rs.getString("k"));
                }
                LOG.debug("Read key: " + key.toString());

                if (rs.next()) {
                    LOG.error("Read key: " + key.asString() + " expected 1 row in result, got more");
                    return 0;
                }
            }
        } catch (Exception e) {
            LOG.info("Failed reading value: " + key.getValueStr(), e);
            close(preparedSelect);
            preparedSelect = null;
            return 0;
        }
        return 1;
    }

    private PreparedStatement getPreparedInsert() throws Exception {
        if (preparedInsert == null) {

            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO ").append(getTableName()).append("(k");
            for (int i = 0; i < appConfig.numValueColumns; i++) {
                String cname = "v" + (i + 1);
                sb.append(", ").append(cname);
            }
            sb.append(") VALUES ( ?");
            for (int i = 0; i < appConfig.numValueColumns; i++) {
                sb.append(", ?");
            }
            sb.append(")");

            close(insConnection);
            insConnection = getPostgresConnection();
            insConnection.createStatement().execute("set yb_enable_upsert_mode = true");
            preparedInsert = insConnection.prepareStatement(sb.toString());
        }
        return preparedInsert;
    }

    private String getValue(Key key, int col_idx, long key_idx) {

        if (col_idx <= appConfig.numForeignKeys) {
            long block_idx = key_idx / appConfig.numConsecutiveRowsWithSameFk;
            long fkey_idx = (block_idx % appConfig.numForeignKeyTableRows) +  1;
            return String.valueOf(fkey_idx);
        }

        return key.getValueStr();
    }

    @Override
    public long doWrite(int threadIdx) {
        HashSet<Key> keys = new HashSet<>();

        try {
            PreparedStatement statement = getPreparedInsert();

            if (appConfig.batchSize > 0) {
                for (int i = 0; i < appConfig.batchSize; i++) {
                    keys.add(setupParams(statement));
                    statement.addBatch();
                }
                statement.executeBatch();
            } else {
                keys.add(setupParams(statement));
                statement.execute();
            }

            for (Key key : keys) {
                getSimpleLoadGenerator().recordWriteSuccess(key);
            }
            return keys.size();

        } catch (Exception e) {
            for (Key key : keys) {
                getSimpleLoadGenerator().recordWriteFailure(key);
            }
            LOG.info("Failed write with error: " + e.getMessage());
            close(preparedInsert);
            preparedInsert = null;
        }
        return keys.size();
    }


    private Key setupParams(PreparedStatement statement) throws Exception {
        Key key = getSimpleLoadGenerator().getKeyToWrite();
        if (key == null) {
            throw new IllegalStateException("Cannot write null key");
        }

        // Prefix hashcode to ensure generated keys are random and not sequential.
        statement.setString(1, key.asString());

        for (int i = 1; i <= appConfig.numValueColumns; i++) {
            statement.setString(i + 1, getValue(key, i, key.asNumber()));
        }

        return key;
    }


    @Override
    public List<String> getWorkloadDescription() {
        return Arrays.asList(
                "Sample app for benchmarking data load into Yugabyte SQL.",
                "Inserts auto-generated rows into a table with various configurable settings: ",
                "number of value columns, number of secondary indexes, number of foreign key ",
                "constraints, batch size, number of write threads, etc.");
    }

    @Override
    public List<String> getWorkloadOptionalArguments() {
        return Arrays.asList(
                "--num_writes " + appConfig.numKeysToWrite,
                "--num_value_columns " + appConfig.numValueColumns,
                "--num_indexes " + appConfig.numIndexes,
                "--num_foreign_keys " + appConfig.numForeignKeys,
                "--num_foreign_key_table_rows " + appConfig.numForeignKeyTableRows,
                "--num_consecutive_rows_with_same_fk " + appConfig.numConsecutiveRowsWithSameFk,
                "--batch_size " + appConfig.batchSize,
                "--num_threads_write " + appConfig.numWriterThreads,
                "--load_balance " + appConfig.loadBalance,
                "--topology_keys " + appConfig.topologyKeys,
                "--debug_driver " + appConfig.enableDriverDebug);
    }
}

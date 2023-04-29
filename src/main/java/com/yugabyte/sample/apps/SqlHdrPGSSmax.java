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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import com.yugabyte.sample.apps.AppBase.TableOp;
import com.yugabyte.sample.common.SimpleLoadGenerator.Key;

public class SqlHdrPGSSmax extends AppBase {
    private static final Logger LOG = Logger.getLogger(SqlHdrPGSSmax.class);
    static {
        // Disable the read-write percentage.
        appConfig.readIOPSPercentage = -1;
        // Set the read and write threads to 1 each.
        appConfig.numReaderThreads = 0;
        appConfig.numWriterThreads = 100;
        // The number of keys to read.
        appConfig.numKeysToRead = 0;
        // The number of keys to write. This is the combined total number of inserts and updates.
        appConfig.numKeysToWrite = 100;
        // The number of unique keys to write. This determines the number of inserts (as opposed to
        // updates).
        appConfig.numUniqueKeysToWrite = 0;
        }

        // The default table name to create and use for CRUD ops.
        private static final String DEFAULT_TABLE_NAME = "SqlHdrPGSSmax";

        // The shared prepared select statement for fetching the data.
        private volatile PreparedStatement preparedSelect = null;

        // The shared prepared insert statement for inserting the data.
        private volatile PreparedStatement preparedInsert = null;
        // The shared prepared insert statement for inserting the data.
        private volatile Connection insConnection = null;

        // Lock for initializing prepared statement objects.
        private static final Object prepareInitLock = new Object();

        public SqlHdrPGSSmax() {
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
        }
        }

        @Override
        public void createTablesIfNeeded(TableOp tableOp) throws Exception {
        try (Connection connection = getPostgresConnection()) {

            // (Re)Create the table (every run should start cleanly with an empty table).
            tableOp = TableOp.DropTable;
            if (tableOp.equals(TableOp.DropTable)) {
                connection.createStatement().execute(
                    String.format("DROP TABLE IF EXISTS %s", getTableName()));
                LOG.info("Dropping table(s) left from previous runs if any");
            }
            connection.createStatement().executeUpdate(
                String.format("CREATE TABLE IF NOT EXISTS %s ();", getTableName()));
            LOG.info(String.format("Created table: %s", getTableName()));

            // if (tableOp.equals(TableOp.TruncateTable)) {
            //     connection.createStatement().execute(
            //     String.format("TRUNCATE TABLE %s", getTableName()));
            // LOG.info(String.format("Truncated table: %s", getTableName()));
            // }

            // // Create an index on the table.
            // connection.createStatement().executeUpdate(
            //     String.format("CREATE INDEX IF NOT EXISTS %s_index ON %s(v);",
            //         getTableName(), getTableName()));
            // LOG.info(String.format("Created index on table: %s", getTableName()));
        }
        }

        public String getTableName() {
        String tableName = appConfig.tableName != null ? appConfig.tableName : DEFAULT_TABLE_NAME;
        return tableName.toLowerCase();
        }

        private PreparedStatement getPreparedSelect() throws Exception {
        if (preparedSelect == null) {
            preparedSelect = getPostgresConnection().prepareStatement(
                String.format("SELECT 1;"));
        }
        return preparedSelect;
        }

        private PreparedStatement getPreparedInsert() throws Exception {
            if (preparedInsert == null) {
                close(insConnection);
                insConnection = getPostgresConnection();
                preparedInsert = insConnection.prepareStatement(
                    String.format("ALTER TABLE %s ADD COLUMN \"?\" INT;", getTableName()));
            }
            return preparedInsert;
        }

        @Override
        public long doWrite(int threadIdx) {
            Key key = getSimpleLoadGenerator().getKeyToWrite();
            if (key == null) {
              return 0;
            }

            int result = 0;
            try {
            // PreparedStatement statement = getPreparedInsert();
            String q = String.format("create table if not exists \"%s\" ();", Integer.toString(threadIdx));
            String d = String.format("drop table if exists \"%s\";", Integer.toString(threadIdx));
            getPostgresConnection().createStatement().execute(d);
            getPostgresConnection().createStatement().execute(q);
            getPostgresConnection().createStatement().execute(d);

            // Prefix hashcode to ensure generated keys are random and not sequential.
            // statement.setString(1, key.asString());
            // statement.setString(1, Integer.toString(threadIdx));
            // result = statement.executeUpdate();
            LOG.info("Created and dropped table: " + Integer.toString(threadIdx) + ", return code: " +
                result);
            getSimpleLoadGenerator().recordWriteSuccess(key);
            } catch (Exception e) {
            getSimpleLoadGenerator().recordWriteFailure(key);
            LOG.info("Failed creating/deleting table: " + Integer.toString(threadIdx), e);
            close(preparedInsert);
            preparedInsert = null;
            return 0;
            }
            return 1;
        }

        @Override
        public long doRead() {
        try {
            PreparedStatement statement = getPreparedSelect();
            try (ResultSet rs = statement.executeQuery()) {
            if (!rs.next()) {
                LOG.error("Select 1: expected 1 row in result, got 0");
                return 0;
            }

            if (rs.next()) {
                LOG.error("Select 1: expected 1 row in result, got more");
                return 0;
            }
            }
        } catch (Exception e) {
            LOG.info("Failed select 1", e);
            preparedSelect = null;
            return 0;
        }
        return 1;
        }

        @Override
        public List<String> getWorkloadDescription() {
        return Arrays.asList(
            "Sample key-value app built on postgresql. The app writes select 1's",
            "Note that the number of reads to perform can be specified as",
            "a parameter.");
        }

        @Override
        public List<String> getWorkloadOptionalArguments() {
        return Arrays.asList(
            "--num_unique_keys " + appConfig.numUniqueKeysToWrite,
            "--num_reads " + appConfig.numKeysToRead,
            "--num_writes " + appConfig.numKeysToWrite,
            "--num_threads_read " + appConfig.numReaderThreads,
            "--num_threads_write " + appConfig.numWriterThreads,
            "--load_balance " + appConfig.loadBalance,
            "--topology_keys " + appConfig.topologyKeys,
            "--debug_driver " + appConfig.enableDriverDebug);
        }
}

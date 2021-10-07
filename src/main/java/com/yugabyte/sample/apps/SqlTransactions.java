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

import com.yugabyte.sample.common.SimpleLoadGenerator.Key;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Workload creates transaction with custom amount of operations in custom amount of tables with savepoints and change to rollback on some of it
 */
public class SqlTransactions extends AppBase {
    private static final Logger LOG = Logger.getLogger(SqlTransactions.class);

    static {
        appConfig.readIOPSPercentage = -1;
        appConfig.numReaderThreads = 1;
        appConfig.numWriterThreads = 1;
        appConfig.numKeysToRead = -1;
        appConfig.numKeysToWrite = -1;
        appConfig.numUniqueKeysToWrite = NUM_UNIQUE_KEYS;
    }

    private static List<InsertedValue> insertedValuesBuffer = new ArrayList<>();

    private static final String DEFAULT_TABLE_NAME = "SqlTransactions";

    class InsertedValue {
        public String key;
        public String tableName;

        InsertedValue(String key, String tableName) {
            this.key = key;
            this.tableName = tableName;
        }
    }

    class Savepoint {
        public int lastSavepointOperationCounter;
        public String name;
        public String tableName;

        Savepoint(String name, int lastSavepointOperationCounter) {
            this.name = name;
            this.lastSavepointOperationCounter = lastSavepointOperationCounter;
        }
    }

    class Cycle {
        private List<String> loopArray;
        private int idx = -1;

        Cycle(List<String> initArray) {
            loopArray = initArray;
        }

        /**
         * Get next vale in received array. If ends then starts from the start
         */
        String next() {
            idx++;
            if (idx == loopArray.size()) {
                idx = 0;
            }
            return loopArray.get(idx);
        }
    }

    class PreparedData {
        public PreparedStatement preparedStatement;
        public List<InsertedValue> insertedValues;

        PreparedData(PreparedStatement preparedStatement, List<InsertedValue> insertedKeys) {
            this.insertedValues = insertedKeys;
            this.preparedStatement = preparedStatement;
        }
    }

    // The shared prepared insert statement for inserting the data.
    public SqlTransactions() {
        buffer = new byte[appConfig.valueSize];
    }

    /**
     * Drop the table created by this app.
     */
    @Override
    public void dropTable() throws Exception {
        try (Connection connection = getPostgresConnection()) {
            for (int i = 1; i < appConfig.numTxTables + 1; i++) {
                connection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s_%s", getTableName(), i));
                LOG.info(String.format("Dropped table: %s", getTableName()));
            }
        }
    }

    @Override
    public void createTablesIfNeeded(TableOp tableOp) throws Exception {
        try (Connection connection = getPostgresConnection()) {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setAutoCommit(false);

            for (int i = 1; i < appConfig.numTxTables + 1; i++) {
                StringBuilder sb = new StringBuilder(String.format("CREATE TABLE IF NOT EXISTS %s_%s (k text PRIMARY KEY ", getTableName(), i));
                IntStream.range(1, appConfig.numValueColumns + 1).forEach(z -> sb.append(String.format(", v%s text", z)));
                sb.append(");");
                connection.createStatement().executeUpdate(sb.toString());
            }
            LOG.info(String.format("Created table: %s", getTableName()));
        }
    }

    public String getTableName() {
        String tableName = appConfig.tableName != null ? appConfig.tableName : DEFAULT_TABLE_NAME;
        return tableName.toLowerCase();
    }

    private String getColumns() {
        StringBuilder sb = new StringBuilder("v1");
        IntStream.range(2, appConfig.numValueColumns + 1).forEach(i -> sb.append(String.format(", v%s", i)));
        return sb.toString();
    }

    @Override
    public long doRead() {
        InsertedValue insertedValue = insertedValuesBuffer.size() == 0 ? null : insertedValuesBuffer.get(random.nextInt(insertedValuesBuffer.size()));
        if (insertedValue == null) {
            return 0;
        }

        try {
            PreparedStatement statement = getPostgresConnection().prepareStatement(
                    String.format("SELECT k, %s FROM %s WHERE k = ?;", getColumns(), insertedValue.tableName)
            );
            statement.setString(1, insertedValue.key);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    LOG.error(String.format("Read key: %s expected 1 row in result, got 0", insertedValue));
                    return 0;
                }

                if (!insertedValue.key.equals(rs.getString("k"))) {
                    LOG.error(String.format("Read key: %s, got %s", insertedValue, rs.getString("k")));
                }
                LOG.debug(String.format("Read key: %s", insertedValue));
            }
        } catch (Exception e) {
            LOG.info(String.format("Failed reading value: %s", insertedValue), e);
            return 0;
        }
        return 1;
    }


    private PreparedData getPreparedInsert(Key key) throws Exception {
        StringBuilder stmt = new StringBuilder("BEGIN TRANSACTION; ");
        List<Savepoint> savepointsHistory = new ArrayList<>();

        String tableNameBase = getTableName();
        List<String> tables = IntStream.range(1, appConfig.numTxTables + 1).mapToObj(i -> String.format("%s_%s", tableNameBase, i)).collect(Collectors.toList());
        Cycle tablesGen = new Cycle(tables);

        int savepointEvery = appConfig.numTxOps / appConfig.numTxSavepoints;

        StringBuilder valuesSb = new StringBuilder("?");
        IntStream.range(1, appConfig.numValueColumns).forEach(i -> valuesSb.append(", ?"));
        String values = valuesSb.toString();

        List<InsertedValue> insertedValues = new ArrayList<>();
        // Create transaction with inserts and savepoints distributed across it
        for (int i = 0; i < appConfig.numTxOps; i++) {
            if (i % savepointEvery == 0) {
                Savepoint point = new Savepoint(String.format("sp_%s", i), insertedValues.size());
                savepointsHistory.add(point);
                stmt.append(String.format("SAVEPOINT %s; ", point.name));
            } else {
                InsertedValue inserted = new InsertedValue(String.format("%s_%s", key.asString(), i), tablesGen.next());
                insertedValues.add(inserted);
                stmt.append(String.format("INSERT INTO %s(k, %s) VALUES (?, %s); ", inserted.tableName, getColumns(), values));
            }
        }

        // chance to rollback on some random savepoint
        Savepoint savepointToRollback = null;
        if (appConfig.numTxSavepoints > 0 && random.nextInt(100) < appConfig.numTxRollbackChange) {
            savepointToRollback = savepointsHistory.get(random.nextInt(savepointsHistory.size()));
            stmt.append(String.format("ROLLBACK TO %s;", savepointToRollback.name));
        } else {
            stmt.append("COMMIT;");
        }

        // set keys and values in prepared statement
        PreparedStatement preparedStatement = getPostgresConnection().prepareStatement(stmt.toString());
        int keysCounter = 0;
        for (InsertedValue insertedValue : insertedValues) {
            keysCounter++;
            preparedStatement.setString(keysCounter, insertedValue.key);
            for (int j = 1; j < appConfig.numValueColumns + 1; j++) {
                keysCounter++;
                preparedStatement.setString(keysCounter, key.getKeyWithHashPrefix());
            }
        }
        // if rollback then add remove rollbacked values
        if (savepointToRollback != null) {
            int lastOperation = savepointToRollback.lastSavepointOperationCounter;
            insertedValues = IntStream.range(0, insertedValues.size()).filter(i -> i <= lastOperation).mapToObj(insertedValues::get).collect(Collectors.toList());
        }
        return new PreparedData(preparedStatement, insertedValues);
    }

    @Override
    public long doWrite(int threadIdx) {
        Key key = getSimpleLoadGenerator().getKeyToWrite();
        if (key == null) {
            return 0;
        }

        int result = 0;
        try {
            PreparedData preparedData = getPreparedInsert(key);
            preparedData.preparedStatement.executeUpdate();
            // add inserted values in buffer
            insertedValuesBuffer.addAll(preparedData.insertedValues);

            try {
                // clean buffer sometimes
                if (insertedValuesBuffer.size() > appConfig.numInsertedKeysBufferSize) {
                    insertedValuesBuffer = IntStream
                            .range(0, insertedValuesBuffer.size())
                            .filter(i -> i % 2 == 0)
                            .mapToObj(insertedValuesBuffer::get)
                            .collect(Collectors.toList());
                }
            } catch (IndexOutOfBoundsException ignored) {
            }

            LOG.debug(String.format("Wrote key: %s return code: %d", key.asString(), result));
            getSimpleLoadGenerator().recordWriteSuccess(key);
            return preparedData.insertedValues.size();
        } catch (Exception e) {
            getSimpleLoadGenerator().recordWriteFailure(key);
            LOG.info("Failed writing key: " + key.asString(), e);
        }
        return 0;
    }

    @Override
    public List<String> getWorkloadDescription() {
        return Arrays.asList(
                "Sample postgresql transactions app. Create transactions with variable amount of insert operations",
                "Each transaction can set custom amount of savepoints with change to restore on random one.");
    }

    @Override
    public List<String> getWorkloadOptionalArguments() {
        return Arrays.asList(
                "--num_unique_keys " + appConfig.numUniqueKeysToWrite,
                "--num_reads " + appConfig.numKeysToRead,
                "--num_writes " + appConfig.numKeysToWrite,
                "--num_threads_read " + appConfig.numReaderThreads,
                "--num_threads_write " + appConfig.numWriterThreads,
                "--num_value_columns " + appConfig.numValueColumns,
                "--num_tx_operations " + appConfig.numTxOps,
                "--num_tx_savepoints " + appConfig.numTxSavepoints,
                "--num_tx_rollback_chance " + appConfig.numTxRollbackChange,
                "--num_inserted_keys_buffer_size " + appConfig.numInsertedKeysBufferSize,
                "--num_tx_tables " + appConfig.numTxTables
        );
    }
}

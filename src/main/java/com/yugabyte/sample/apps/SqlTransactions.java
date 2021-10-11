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
import java.util.Collections;
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
        public List<String> values;
        public String tableName;
        public Operation operation;
        public boolean exists = true;

        InsertedValue(String key, String tableName, List<String> values, Operation operation) {
            this.key = key;
            this.tableName = tableName;
            this.values = values;
            this.operation = operation;
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

    enum Operation {
        INSERT, DELETE, UPDATE
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

    private List<String> getColumns() {
        return IntStream.range(1, appConfig.numValueColumns + 1).mapToObj(i -> String.format("v%s", i)).collect(Collectors.toList());
    }

    private String getColumnsStr() {
        return String.join(", ", getColumns());
    }

    @Override
    public long doRead() {
        InsertedValue insertedValue = insertedValuesBuffer.size() == 0 ? null : insertedValuesBuffer.get(random.nextInt(insertedValuesBuffer.size()));
        if (insertedValue == null) {
            return 0;
        }

        try (Connection connection = getPostgresConnection()) {
            PreparedStatement statement = connection.prepareStatement(
                    String.format("SELECT k, %s FROM %s WHERE k = ?;", getColumnsStr(), insertedValue.tableName)
            );
            statement.setString(1, insertedValue.key);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    if (!insertedValue.exists) {
                        return 1;
                    }
                    LOG.error(String.format("Read key: %s expected 1 row in result, got 0", insertedValue.key));
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


    private PreparedData getPreparedTx(Connection connection, Key key) throws Exception {
        StringBuilder stmt = new StringBuilder("BEGIN TRANSACTION; ");
        List<Savepoint> savepointsHistory = new ArrayList<>();

        String tableNameBase = getTableName();
        List<String> tables = IntStream.range(1, appConfig.numTxTables + 1).mapToObj(i -> String.format("%s_%s", tableNameBase, i)).collect(Collectors.toList());
        Cycle tablesGen = new Cycle(tables);

        int savepointEvery = appConfig.numTxOps / appConfig.numTxSavepoints;
        List<Operation> operations = new ArrayList<>();
        if (insertedValuesBuffer.size() > 0) {
            IntStream.range(0, appConfig.numTxUpdates).forEach(i -> operations.add(Operation.UPDATE));
            IntStream.range(0, appConfig.numTxDeletes).forEach(i -> operations.add(Operation.DELETE));
        }
        IntStream.range(0, appConfig.numTxOps - operations.size()).forEach(i -> operations.add(Operation.INSERT));
        Collections.shuffle(operations);

        StringBuilder valuesSb = new StringBuilder("?");
        IntStream.range(1, appConfig.numValueColumns).forEach(i -> valuesSb.append(", ?"));
        String values = valuesSb.toString();

        List<InsertedValue> ddlOperations = new ArrayList<>();
        // Create transaction with inserts and savepoints distributed across it
        int counter = -1;
        List<String> allColumns = getColumns();
        String allColumnsStr = String.join(", ", allColumns);

        for (Operation operation : operations) {
            counter++;
            if (counter % savepointEvery == 0) {
                Savepoint point = new Savepoint(String.format("sp_%s", counter), ddlOperations.size());
                savepointsHistory.add(point);
                stmt.append(String.format("SAVEPOINT %s; ", point.name));
            } else {
                InsertedValue value;
                List<String> valuesToInsert = new ArrayList<>();
                switch (operation) {
                    case INSERT:
                        for (int v = 0; v < appConfig.numValueColumns; v++) valuesToInsert.add(key.getKeyWithHashPrefix());
                        value = new InsertedValue(String.format("%s_%s", key.asString(), counter), tablesGen.next(), valuesToInsert, operation);
                        ddlOperations.add(value);
                        stmt.append(String.format("INSERT INTO %s(k, %s) VALUES (?, %s); ", value.tableName, allColumnsStr, values));
                        break;
                    case UPDATE:
                        if (insertedValuesBuffer.size() == 0) {
                            continue;
                        }
                        InsertedValue valueToUpdate = null;
                        for (int i = 0; i < 100; i++) {
                            valueToUpdate = insertedValuesBuffer.get(random.nextInt(insertedValuesBuffer.size()));
                            if (valueToUpdate.exists) {
                                break;
                            }
                        }
                        if (!valueToUpdate.exists) {
                            break;
                        }

                        String columnToUpdate = allColumns.get(random.nextInt(valueToUpdate.values.size()));
                        valuesToInsert.add(valueToUpdate.key);
                        valuesToInsert.add(key.getKeyWithHashPrefix());
                        value = new InsertedValue(String.format("%s_%s", key.asString(), counter), tablesGen.next(), valuesToInsert, operation);
                        ddlOperations.add(value);
                        stmt.append(String.format("UPDATE %s SET %s=? WHERE k = ?; ", valueToUpdate.tableName, columnToUpdate));
                        break;
                    case DELETE:
                        if (insertedValuesBuffer.size() == 0) {
                            continue;
                        }
                        InsertedValue valueToDelete = null;
                        int idxToDelete = 0;
                        for (int i = 0; i < 100; i++) {
                            idxToDelete = random.nextInt(insertedValuesBuffer.size());
                            valueToDelete = insertedValuesBuffer.get(idxToDelete);
                            if (valueToDelete.exists) {
                                break;
                            }
                        }
                        if (!valueToDelete.exists) {
                            break;
                        }
                        try {
                            insertedValuesBuffer.get(idxToDelete).exists = false;
                        } catch (Exception ignored) {
                        }
                        valuesToInsert.add(valueToDelete.key);
                        value = new InsertedValue(String.format("%s_%s", key.asString(), counter), tablesGen.next(), valuesToInsert, operation);
                        ddlOperations.add(value);
                        stmt.append(String.format("DELETE FROM %s WHERE k = ?; ", value.tableName));
                        break;
                }
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
        PreparedStatement preparedStatement = connection.prepareStatement(stmt.toString());
        int keysCounter = 0;
        for (InsertedValue insertedValue : ddlOperations) {
            switch (insertedValue.operation) {
                case INSERT:
                    keysCounter++;
                    try {
                        preparedStatement.setString(keysCounter, insertedValue.key);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    for (String valueToInsert : insertedValue.values) {
                        keysCounter++;
                        preparedStatement.setString(keysCounter, valueToInsert);
                    }
                    break;
                case UPDATE:
                    // WHERE
                    keysCounter++;
                    preparedStatement.setString(keysCounter, insertedValue.values.get(0));
                    // SET
                    keysCounter++;
                    preparedStatement.setString(keysCounter, insertedValue.values.get(1));
                    break;
                case DELETE:
                    keysCounter++;
                    preparedStatement.setString(keysCounter, insertedValue.values.get(0));
                    break;
            }
        }
        // if rollback then add remove rollbacked values
        if (savepointToRollback != null) {
            int lastOperation = savepointToRollback.lastSavepointOperationCounter;
            ddlOperations = IntStream
                    .range(0, ddlOperations.size())
                    .filter(i -> i <= lastOperation)
                    .mapToObj(ddlOperations::get)
                    .filter(o -> o.operation == Operation.INSERT)
                    .collect(Collectors.toList());
        } else {
            ddlOperations = ddlOperations.stream().filter(o -> o.operation == Operation.INSERT).collect(Collectors.toList());
        }
        return new PreparedData(preparedStatement, ddlOperations);
    }

    @Override
    public long doWrite(int threadIdx) {
        Key key = getSimpleLoadGenerator().getKeyToWrite();
        if (key == null) {
            return 0;
        }

        int result = 0;
        PreparedData preparedData;
        try (Connection connection = getPostgresConnection()) {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setAutoCommit(false);
            preparedData = getPreparedTx(connection, key);
            preparedData.preparedStatement.executeUpdate();
            // add inserted values in buffer
            insertedValuesBuffer.addAll(preparedData.insertedValues);

            // clean buffer sometimes
            if (insertedValuesBuffer.size() > appConfig.numInsertedKeysBufferSize) {
                insertedValuesBuffer = IntStream
                        .range(0, insertedValuesBuffer.size())
                        .filter(i -> i % 2 == 0 || !insertedValuesBuffer.get(i).exists)
                        .mapToObj(insertedValuesBuffer::get)
                        .collect(Collectors.toList());
            }
        } catch (Exception e) {
            LOG.error(String.format("failed to write %s", e.getMessage()));
            e.printStackTrace();
        }

        LOG.debug(String.format("Wrote key: %s return code: %d", key.asString(), result));
        getSimpleLoadGenerator().recordWriteSuccess(key);
        return 1;
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
                "--num_tx_updates " + appConfig.numTxUpdates,
                "--num_tx_deletes " + appConfig.numTxDeletes,
                "--num_inserted_keys_buffer_size " + appConfig.numInsertedKeysBufferSize,
                "--num_tx_tables " + appConfig.numTxTables
        );
    }
}

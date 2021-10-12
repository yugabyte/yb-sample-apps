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
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Workload creates transaction with custom amount of operations in custom amount of tables with savepoints and change to rollback on some of it
 */
public class SqlTransactions extends AppBase {
    private static final Logger LOG = Logger.getLogger(SqlTransactions.class);
    private static final String DEFAULT_TABLE_NAME = "SqlTransactions";
    private static List<InsertedValue> insertedValuesBuffer = new ArrayList<>();

    static {
        appConfig.readIOPSPercentage = -1;
        appConfig.numReaderThreads = 1;
        appConfig.numWriterThreads = 1;
        appConfig.numKeysToRead = -1;
        appConfig.numKeysToWrite = -1;
        appConfig.numUniqueKeysToWrite = NUM_UNIQUE_KEYS;
    }

    static List<ConnectionStore> connections = new ArrayList<>();
    HashMap<String, PreparedStatement> statement = new HashMap<>();

    // The shared prepared insert statement for inserting the data.
    public SqlTransactions() {
        buffer = new byte[appConfig.valueSize];
    }

    /**
     * Drop the table created by this app.
     */
    @Override
    public void dropTable() throws Exception {
        ConnectionStore connectionStore = getRandomConnection();
        try {
            for (int i = 1; i < appConfig.numTxTables + 1; i++) {
                connectionStore.connection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s_%s", getTableName(), i));
                LOG.info(String.format("Dropped table: %s", getTableName()));
            }
        } finally {
            backConnectionStore(connectionStore);
        }
    }

    @Override
    public void createTablesIfNeeded(TableOp tableOp) throws Exception {
        ConnectionStore connectionStore = getRandomConnection();
        try {
            for (int i = 1; i < appConfig.numTxTables + 1; i++) {
                StringBuilder sb = new StringBuilder(String.format("CREATE TABLE IF NOT EXISTS %s_%s (k text PRIMARY KEY ", getTableName(), i));
                IntStream.range(1, appConfig.numValueColumns + 1).forEach(z -> sb.append(String.format(", v%s text", z)));
                sb.append(");");
                connectionStore.connection.createStatement().executeUpdate(sb.toString());
            }
            LOG.info(String.format("Created table: %s", getTableName()));
        } finally {
            backConnectionStore(connectionStore);
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

    private void prepareSelect(Connection connection, String tableName) {
        if (!statement.containsKey(tableName)) {
            try {
                statement.put(tableName, connection.prepareStatement(
                        String.format("SELECT k, %s FROM %s WHERE k = ?;", getColumnsStr(), tableName)
                ));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void ensureConnection() {
        if (connections.stream().filter(c -> c.dead).count() > 10) {
            connections.forEach(c -> {
                try {
                    c.connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
            connections = connections.stream().filter(c -> !c.dead).collect(Collectors.toList());
        }
        long activeConnectionsCount = connections.stream().filter(c -> !c.active).count();
        if (activeConnectionsCount < 10) {
            for (int i = 0; i < 10 - activeConnectionsCount; i++) {
                connections.add(new ConnectionStore());
            }
        }
    }

    private ConnectionStore getRandomConnection() {
        while (true) {
            ensureConnection();
            Optional<ConnectionStore> anyConnection = connections.stream().filter(c -> !c.active).findAny();
            if (anyConnection.isPresent()) {
                ConnectionStore store = anyConnection.get();
                store.active = true;
                return store;
            }
        }
    }

    private void backConnectionStore(ConnectionStore store) {
        Optional<ConnectionStore> foundStore = connections.stream().filter(s -> s.id == store.id).findAny();
        foundStore.ifPresent(connectionStore -> connectionStore.active = false);
    }

    @Override
    public long doRead() {
        InsertedValue insertedValue = insertedValuesBuffer.size() == 0 ? null : insertedValuesBuffer.get(random.nextInt(insertedValuesBuffer.size()));
        if (insertedValue == null) {
            return 0;
        }

        ConnectionStore connectionStore = getRandomConnection();
        try {
            prepareSelect(connectionStore.connection, insertedValue.tableName);
            PreparedStatement statement = this.statement.get(insertedValue.tableName);
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
            LOG.error(String.format("Failed reading value 2: %s", insertedValue));
            insertedValuesBuffer = insertedValuesBuffer.stream().filter(b -> !b.key.equals(insertedValue.key)).collect(Collectors.toList());
            e.printStackTrace();
            return 0;
        } finally {
            backConnectionStore(connectionStore);
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

        List<String> updatedKeys = new ArrayList<>();
        List<String> deletedKeys = new ArrayList<>();

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
                        for (int v = 0; v < appConfig.numValueColumns; v++) valuesToInsert.add(key.getValueStr());
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
                            if (valueToUpdate.exists && !updatedKeys.contains(valueToUpdate.key) && !deletedKeys.contains(valueToUpdate.key)) {
                                break;
                            }
                        }
                        if (!valueToUpdate.exists) {
                            break;
                        }

                        String columnToUpdate = allColumns.get(random.nextInt(valueToUpdate.values.size()));
                        updatedKeys.add(valueToUpdate.key);
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
                            if (valueToDelete.exists && !updatedKeys.contains(valueToDelete.key) && !deletedKeys.contains(valueToDelete.key)) {
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
                        deletedKeys.add(valueToDelete.key);
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
    public void terminate() {
        connections.forEach(connectionStore -> {
            try {
                connectionStore.connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
        super.terminate();
    }

    @Override
    public long doWrite(int threadIdx) {

        Key key = getSimpleLoadGenerator().getKeyToWrite();
        if (key == null) {
            return 0;
        }

        ConnectionStore connectionStore = getRandomConnection();
        try {
            PreparedData preparedData = getPreparedTx(connectionStore.connection, key);
            preparedData.preparedStatement.executeUpdate();
            // add inserted values in buffer
            insertedValuesBuffer.addAll(preparedData.insertedValues);

            // clean buffer sometimes
            if (insertedValuesBuffer.size() > appConfig.numInsertedKeysBufferSize) {
                insertedValuesBuffer = IntStream
                        .range(0, insertedValuesBuffer.size())
                        .filter(i -> i % 2 == 0)
                        .mapToObj(insertedValuesBuffer::get)
                        .filter(item -> item.exists)
                        .collect(Collectors.toList());
            }
        } catch (SQLException e) {
            connectionStore.dead = true;
            LOG.error(String.format("failed to write %s %s", key.asString(), e.getMessage()));
            e.printStackTrace();
            sleep(2000);
        } catch (Exception e) {
            LOG.error(String.format("failed to write %s", e.getMessage()));
            e.printStackTrace();
        } finally {
            backConnectionStore(connectionStore);
        }

        LOG.debug(String.format("Wrote key: %s return code: %d", key.asString(), 1));
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

    enum Operation {
        INSERT, DELETE, UPDATE
    }

    class ConnectionStore {
        public Connection connection;
        public boolean active;
        public boolean dead;
        public UUID id;

        ConnectionStore() {
            active = false;
            dead = false;
            id = UUID.randomUUID();
            try {
                connection = getPostgresConnection();
            } catch (Exception e) {
                LOG.error(String.format("Failed to create connection store: %s", e.getMessage()));
                e.printStackTrace();
            }
        }
    }

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

        @Override
        public String toString() {
            return "InsertedValue{" +
                    "key='" + key + '\'' +
                    ", values=" + values +
                    ", tableName='" + tableName + '\'' +
                    ", operation=" + operation +
                    ", exists=" + exists +
                    '}';
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
        private final List<String> loopArray;
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
}

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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Logger;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;

import com.yugabyte.sample.common.CmdLineOpts;

/**
 * A sample IoT event data application with batch processing
 *
 * This app tracks a bunch of devices which have a series of data points ordered
 * by timestamp. The device id can be thought of as a compound/concatenated
 * unique id - which could include user id, on a node id and device id (and id
 * of the device). Every device can have different types of event codes
 * generated.
 */
public class CassandraEventData extends AppBase {
	private static final Logger LOG = Logger.getLogger(CassandraEventData.class);
	// Static initialization of this workload's config.
	static {
		// Set the default number of reader and writer threads.
		appConfig.numReaderThreads = 2;
		appConfig.numWriterThreads = 16;
		// Set the number of keys to read and write.
		appConfig.numKeysToRead = -1;
		appConfig.numKeysToWrite = -1;
		// The size of the value.
		appConfig.valueSize = 3000;
		// Set the TTL for the raw table.
		appConfig.tableTTLSeconds = 24 * 60 * 60;
		// Default write batch size.
		appConfig.batchSize = 25;
		// Default read batch size.
		appConfig.cassandraReadBatchSize = 25;
		// Default time delta from current time to read in a batch.
		appConfig.readBackDeltaTimeFromNow = 180;
		// Number of devices to simulate data for CassandraEventData workload
		appConfig.num_devices = 100;
		// Number of Event Types per device to simulate data for CassandraEventData workload
		appConfig.num_event_types = 100;

	}

	static Random random = new Random();
	// The default table name that has the raw device data.
	private final String DEFAULT_TABLE_NAME = "event_data_raw";
	// The structure to hold info per device.
	static List<DataSource> dataSources = new CopyOnWriteArrayList<DataSource>();
	// The shared prepared select statement for fetching the data.
	private static volatile PreparedStatement preparedSelect;
	// The shared prepared statement for inserting into the table.
	private static volatile PreparedStatement preparedInsert;
	// Lock for initializing prepared statement objects.
	private static final Object prepareInitLock = new Object();

	@Override
	public void initialize(CmdLineOpts configuration) {
		synchronized (dataSources) {
			// If the datasources have already been created, we have already initialized the
			// static
			// variables, so nothing to do.
			if (!dataSources.isEmpty()) {
				return;
			}

			// Create all the device data sources.
			for (int i = 0; i < appConfig.num_devices; i++) {
				DataSource dataSource = new DataSource(i);
				dataSources.add(dataSource);
			}
		}
	}

	public String getTableName() {
		return appConfig.tableName != null ? appConfig.tableName : DEFAULT_TABLE_NAME;
	}

	@Override
	public void dropTable() {
		dropCassandraTable(getTableName());
	}

	@Override
	protected List<String> getCreateTableStatements() {

		return Arrays.asList(String.format("CREATE TABLE IF NOT EXISTS %s %s %s %s %s;", getTableName(),
				" ( device_id varchar , ts bigint , event_type varchar , value blob , primary key (device_id, ts, event_type))",
				" WITH CLUSTERING ORDER BY (ts DESC, event_type ASC)", " AND default_time_to_live = ",
				appConfig.tableTTLSeconds),
				String.format("CREATE INDEX IF NOT EXISTS search_by_event_type ON %s %s %s %s;", getTableName(),
						" ( device_id, event_type, ts ) ", " WITH CLUSTERING ORDER BY (event_type ASC, ts DESC)",
						"AND transactions = { 'enabled' : false, 'consistency_level' : 'user_enforced' }"));

	}

	private PreparedStatement getPreparedInsert() {
		if (preparedInsert == null) {
			synchronized (prepareInitLock) {
				if (preparedInsert == null) {
					// Create the prepared statement object.
					String insert_stmt = String.format("INSERT INTO %s (device_id, ts, event_type, value) VALUES "
							+ "(:device_id, :ts, :event_type, :value);", getTableName());
					preparedInsert = getCassandraClient().prepare(insert_stmt);
				}
			}
		}
		return preparedInsert;
	}

	private PreparedStatement getPreparedSelect() {
		if (preparedSelect == null) {
			synchronized (prepareInitLock) {
				if (preparedSelect == null) {
					// Create the prepared statement object.
					String select_stmt = String.format("SELECT * from %s WHERE device_id = :deviceId AND "
							+ "ts > :startTs AND ts < :endTs AND event_type=:event_type ORDER BY ts DESC "
							+ "LIMIT :readBatchSize;", getTableName());
					preparedSelect = getCassandraClient().prepare(select_stmt);
				}
			}
		}
		return preparedSelect;
	}

	@Override
	public synchronized void resetClients() {
		synchronized (prepareInitLock) {
			preparedInsert = null;
			preparedSelect = null;
		}
		super.resetClients();
	}

	@Override
	public synchronized void destroyClients() {
		synchronized (prepareInitLock) {
			preparedInsert = null;
			preparedSelect = null;
		}
		super.destroyClients();
	}

	@Override
	public long doRead() {
		// Pick a random data source.
		DataSource dataSource = dataSources.get(random.nextInt(dataSources.size()));
		// Make sure it has emitted data, otherwise there is nothing to read.
		if (!dataSource.getHasEmittedData()) {
			return 0;
		}
		long startTs = dataSource.getStartTs();
		long endTs = dataSource.getEndTs();

		// Bind the select statement.
		BoundStatement select = getPreparedSelect().bind().setString("deviceId", dataSource.getDeviceId())
				.setLong("startTs", startTs).setLong("endTs", endTs).setString("event_type", dataSource.getEventType())
				.setInt("readBatchSize", appConfig.cassandraReadBatchSize);
		// Make the query.
		ResultSet rs = getCassandraClient().execute(select);
		return 1;
	}

	private ByteBuffer getValue(String device_id) {

		byte[] randBytesArr = new byte[appConfig.valueSize];

		getRandomValue(device_id.getBytes(), appConfig.valueSize, randBytesArr);

		return ByteBuffer.wrap(randBytesArr);
	}

	@Override
	public long doWrite(int threadIdx) {
		// Pick a random data source.
		DataSource dataSource = dataSources.get(random.nextInt(dataSources.size()));
		long numKeysWritten = 0;

		BatchStatement batch = new BatchStatement();
		// Enter a batch of data points.
		long ts = dataSource.getDataEmitTs();
		for (int i = 0; i < appConfig.batchSize; i++) {
			batch.add(getPreparedInsert().bind().setString("device_id", dataSource.getDeviceId()).setLong("ts", ts)
					.setString("event_type", dataSource.getEventType())
					.setBytesUnsafe("value", getValue(dataSource.getDeviceId())));
			numKeysWritten++;
			ts++;
		}
		dataSource.setLastEmittedTs(ts);

		getCassandraClient().execute(batch);

		return numKeysWritten;
	}

	/**
	 * This class represents a single device data source, which sends back
	 * timeseries data for that device. It generates data governed by the emit rate.
	 */
	public static class DataSource {
		// The list of devices to emit for this data source.
		private String device_id;
		// The timestamp at which the data emit started.
		private long dataEmitStartTs = 1;
		// State variable tracking the last time emitted by this source.
		private long lastEmittedTs = -1;
		// Event Type of the data record
		private String event_type;

		public DataSource(int index) {
			this.device_id = String.format("device-%05d", index);
		}

		public String getDeviceId() {
			return device_id;
		}

		public boolean getHasEmittedData() {
			return lastEmittedTs >= dataEmitStartTs;
		}

		public long getEndTs() {
			return getLastEmittedTs() + 1;
		}

		public long getStartTs() {
			return getEndTs() > appConfig.readBackDeltaTimeFromNow ? getEndTs() - appConfig.readBackDeltaTimeFromNow
					: dataEmitStartTs;
		}

		public long getDataEmitTs() {
			if (lastEmittedTs == -1) {
				lastEmittedTs = dataEmitStartTs;
			}
			return lastEmittedTs;
		}

		public synchronized long getLastEmittedTs() {
			return lastEmittedTs;
		}

		public synchronized void setLastEmittedTs(long ts) {
			if (lastEmittedTs < ts) {
				lastEmittedTs = ts;
			}
		}

		@Override
		public String toString() {
			return getDeviceId();
		}

		public String getEventType() {
			return Integer.toString(random.nextInt(appConfig.num_event_types));
		}

	}

	@Override
	public List<String> getWorkloadDescription() {
		return Arrays.asList("Timeseries/IoT app built that simulates device data emitted by devices periodically. ",
				"The data is written into the 'event_data_raw' table, which retains data for one day.",
				"Note that the number of devices written is a lot more than the number of devices read as ",
				"is typical in such workloads, and the payload size for each write can be configurable(in bytes). Every ",
				"read query fetches the a limited batch from recently written values for a random device.");
	}

	@Override
	public List<String> getWorkloadOptionalArguments() {
		return Arrays.asList("--num_threads_read " + appConfig.numReaderThreads,
				"--num_threads_write " + appConfig.numWriterThreads, "--num_devices " + appConfig.num_devices,
				"--num_event_types " + appConfig.num_event_types, "--table_ttl_seconds " + appConfig.tableTTLSeconds,
				"--batch_size " + appConfig.batchSize,
				"--read_batch_size " + appConfig.cassandraReadBatchSize);
	}
}

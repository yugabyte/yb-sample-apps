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

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.yugabyte.sample.common.metrics.Observation;
import com.yugabyte.sample.common.metrics.PromMetrics;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.yugabyte.driver.core.policies.PartitionAwarePolicy;
import com.yugabyte.sample.common.CmdLineOpts;
import com.yugabyte.sample.common.CmdLineOpts.ContactPoint;
import com.yugabyte.sample.common.RedisHashLoadGenerator;
import com.yugabyte.sample.common.SimpleLoadGenerator;
import com.yugabyte.sample.common.SimpleLoadGenerator.Key;
import com.yugabyte.sample.common.metrics.MetricsTracker;
import com.yugabyte.sample.common.metrics.MetricsTracker.MetricName;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.YBJedis;

/**
 * Abstract base class for all apps. This class does the following:
 *   - Provides various helper methods including methods for creating YSQL, YCQL and Redis clients.
 *   - Has a metrics tracker object, and internally tracks reads and writes.
 *   - Has the abstract methods that are implemented by the various apps.
 */
public abstract class AppBase implements MetricsTracker.StatusMessageAppender {
  private static final Logger LOG = Logger.getLogger(AppBase.class);

  // Number of uniques keys to insert by default.
  public static final int NUM_UNIQUE_KEYS = 1000000;

  // Number of uniques keys to insert by default for YSQL and YCQL.
  public static final int NUM_UNIQUE_KEYS_FOR_YSQL_AND_YCQL = 2000000;

  // Default Number of keys to write for YSQL and YCQL.
  public static final int NUM_KEYS_TO_WRITE_FOR_YSQL_AND_YCQL = 2000000;

  // Default Number of keys to read for YSQL and YCQL.
  public static final int NUM_KEYS_TO_READ_FOR_YSQL_AND_YCQL = 1500000;

  // Variable to track start time of the workload.
  private long workloadStartTime = -1;
  // Instance of the workload configuration.
  public static AppConfig appConfig = new AppConfig();
  // The configuration of the load tester.
  protected CmdLineOpts configuration;
  // The number of keys written so far.
  protected static AtomicLong numKeysWritten = new AtomicLong(0);
  // The number of keys that have been read so far.
  protected static AtomicLong numKeysRead = new AtomicLong(0);
  // Object to track read and write metrics.
  private static volatile MetricsTracker metricsTracker;
  // State variable to track if this workload has finished.
  protected AtomicBoolean hasFinished = new AtomicBoolean(false);
  // The Cassandra client variables.
  protected static volatile Cluster cassandra_cluster = null;
  protected static volatile Session cassandra_session = null;
  // The Java redis client.
  private volatile Jedis jedisClient = null;
  private volatile YBJedis ybJedisClient = null;
  private volatile Pipeline jedisPipeline = null;
  private List<ContactPoint> redisServerInUse = null;
  private volatile JedisCluster jedisCluster = null;
  // Instances of the load generator.
  private static volatile SimpleLoadGenerator simpleLoadGenerator = null;
  private static volatile RedisHashLoadGenerator redisHashLoadGenerator = null;

  // Is this app instance the main instance?
  private boolean mainInstance = false;

  // YCQL keyspace name.
  public static String keyspace = "ybdemo_keyspace";

  // Prometheus metrics.
  private static PromMetrics promMetrics;

  public enum TableOp {
    NoOp,
    DropTable,
    TruncateTable,
  }

  //////////// Helper methods to return the client objects (Redis, Cassandra, etc). ////////////////

  /**
   * We create one shared Cassandra client. This is a non-synchronized method, so multiple threads
   * can call it without any performance penalty. If there is no client, a synchronized thread is
   * created so that exactly only one thread will create a client. If there is a pre-existing
   * client, we just return it.
   * @return a Cassandra Session object.
   */
  protected Session getCassandraClient() {
    if (cassandra_session == null) {
      createCassandraClient(configuration.getContactPoints());
    }
    return cassandra_session;
  }

  protected Connection getPostgresConnection() throws Exception {
    return getPostgresConnection(appConfig.defaultPostgresDatabase);
  }

  protected Connection getPostgresConnection(String database) throws Exception {
    if (appConfig.dbUsername == null) {
      return getPostgresConnection(database, appConfig.defaultPostgresUsername, null);
    }
    return getPostgresConnection(database, appConfig.dbUsername, appConfig.dbPassword);
  }

  // Returns false if the sleep is interrupted
  static boolean sleep(int durationMs) {
  	try {
      Thread.sleep(durationMs);
    } catch (InterruptedException ie) {
  	  return false;
    }
  	return true;
  }

  protected Connection getPostgresConnection(String database, String username, String password) throws Exception {
    boolean isLoadBalanceOn = Boolean.valueOf(String.valueOf(appConfig.loadBalance));
    if (isLoadBalanceOn) {
      Class.forName("com.yugabyte.Driver");
    } else {
      Class.forName("org.postgresql.Driver");
    }
    do {
      try {
        ContactPoint contactPoint = getRandomContactPoint();
        Properties props = new Properties();
        props.setProperty("user", username);
        if (isLoadBalanceOn) {
          props.setProperty("load-balance", String.valueOf(appConfig.loadBalance));
          if (appConfig.topologyKeys != null) {
            props.setProperty("topology-keys", appConfig.topologyKeys);
          }
        }

        if (appConfig.enableDriverDebug) {
          props.setProperty("loggerLevel", "debug");
        }

        if (password != null) {
          props.setProperty("password", password);
        }
        props.setProperty("reWriteBatchedInserts", "true");
        if (appConfig.localReads) {
          props.setProperty("options", "-c yb_read_from_followers=true");
        }

        if (appConfig.sslCert != null && appConfig.sslCert.length() > 0) {
          assert(appConfig.sslKey != null && appConfig.sslKey.length() > 0) : "The SSL key is empty.";
          props.put("sslmode", "require");
          props.put("sslcert", appConfig.sslCert);
          props.put("sslkey", appConfig.sslKey);
        }

        String url = isLoadBalanceOn ? "jdbc:yugabytedb:" : "jdbc:postgresql:";
        String connectStr = String.format("%s//%s:%d/%s", url, contactPoint.getHost(),
                contactPoint.getPort(),
                database);
        Connection connection = DriverManager.getConnection(connectStr, props);
        return connection;
      } catch (Exception e) {
        LOG.info("Going to retrieve connection again: " + e.getMessage());
        if (!sleep(2000)) return null;
        continue;
      }
    } while (true);
  }

  public void initializeConnectionsAndStatements(int numThreads) { }

  protected static void createKeyspace(Session session, String ks) {
    if (!appConfig.skipDDL) {
      String create_keyspace_stmt = "CREATE KEYSPACE IF NOT EXISTS " + ks +
              " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 1};";
      // Use consistency level ONE to allow cross DC requests.
      session.execute(
              session.prepare(create_keyspace_stmt)
                      .setConsistencyLevel(ConsistencyLevel.ONE)
                      .bind());
      LOG.debug("Created a keyspace " + ks + " using query: [" + create_keyspace_stmt + "]");
    }
    String use_keyspace_stmt = "USE " + ks + ";";
    session.execute(
      session.prepare(use_keyspace_stmt)
        .setConsistencyLevel(ConsistencyLevel.ONE)
        .bind());
    LOG.debug("Used the new keyspace " + ks + " using query: [" + use_keyspace_stmt + "]");
  }

  protected String getKeyspace() {
    return keyspace;
  }

  /**
   * Private function that returns an SSL Handler when provided with a cert file.
   * Used for creating a secure connection for the app.
   */
  private SSLOptions createSSLHandler(String certfile) {
    try {
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      FileInputStream fis = new FileInputStream(certfile);
      X509Certificate ca;
      try {
        ca = (X509Certificate) cf.generateCertificate(fis);
      } catch (Exception e) {
        LOG.error("Exception generating certificate from input file: ", e);
        return null;
      } finally {
        fis.close();
      }

      // Create a KeyStore containing our trusted CAs
      String keyStoreType = KeyStore.getDefaultType();
      KeyStore keyStore = KeyStore.getInstance(keyStoreType);
      keyStore.load(null, null);
      keyStore.setCertificateEntry("ca", ca);

      // Create a TrustManager that trusts the CAs in our KeyStore
      String tmfAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
      TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
      tmf.init(keyStore);

      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(null, tmf.getTrustManagers(), null);
      return JdkSSLOptions.builder().withSSLContext(sslContext).build();
    } catch (Exception e) {
      LOG.error("Exception creating sslContext: ", e);
      return null;
    }
  }

  /**
   * Private method that is thread-safe and creates the Cassandra client. Exactly one calling thread
   * will succeed in creating the client. This method does nothing for the other threads.
   * @param contactPoints list of contact points for the client.
   */
  protected synchronized void createCassandraClient(List<ContactPoint> contactPoints) {
    Cluster.Builder builder;
    if (cassandra_cluster == null) {
      builder = Cluster.builder();
      if (appConfig.dbUsername != null) {
        if (appConfig.dbPassword == null) {
          throw new IllegalArgumentException("Password required when providing a username");
        }
        builder = builder
            .withCredentials(appConfig.dbUsername, appConfig.dbPassword);
      }
      if (appConfig.sslCert != null) {
        builder = builder
            .withSSL(createSSLHandler(appConfig.sslCert));
      }
      Integer port = null;
      SocketOptions socketOptions = new SocketOptions();
      if (appConfig.cqlConnectTimeoutMs > 0) {
        socketOptions.setConnectTimeoutMillis(appConfig.cqlConnectTimeoutMs);
      }
      if (appConfig.cqlReadTimeoutMs > 0) {
        socketOptions.setReadTimeoutMillis(appConfig.cqlReadTimeoutMs);
      }
      builder.withSocketOptions(socketOptions);
      for (ContactPoint cp : contactPoints) {
        if (port == null) {
          port = cp.getPort();
          builder.withPort(port);
        } else if (port != cp.getPort()) {
          throw new IllegalArgumentException("Using multiple CQL ports is not supported.");
        }
        builder.addContactPoint(cp.getHost());
      }
      LOG.info("Connecting with " + appConfig.concurrentClients + " clients to nodes: "
          + builder.getContactPoints()
              .stream().map(it -> it.toString()).collect(Collectors.joining(",")));
      PoolingOptions poolingOptions = new PoolingOptions();
      poolingOptions
          .setCoreConnectionsPerHost(HostDistance.LOCAL, appConfig.concurrentClients)
          .setMaxConnectionsPerHost(HostDistance.LOCAL, appConfig.concurrentClients)
          .setCoreConnectionsPerHost(HostDistance.REMOTE, appConfig.concurrentClients)
          .setMaxConnectionsPerHost(HostDistance.REMOTE, appConfig.concurrentClients);
      cassandra_cluster =
          builder.withLoadBalancingPolicy(getLoadBalancingPolicy())
                 .withPoolingOptions(poolingOptions)
                 .withQueryOptions(new QueryOptions()
                     .setDefaultIdempotence(true)
                     .setConsistencyLevel(ConsistencyLevel.QUORUM))
                 .withRetryPolicy(new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE))
                 .build();
      LOG.debug("Connected to cluster: " + cassandra_cluster.getClusterName());
    }
    if (cassandra_session == null) {
      LOG.debug("Creating a session...");
      cassandra_session = cassandra_cluster.connect();
      createKeyspace(cassandra_session, keyspace);
    }
  }

  protected LoadBalancingPolicy getLoadBalancingPolicy() {
    LoadBalancingPolicy policy;
    if (appConfig.localDc != null && !appConfig.localDc.isEmpty()) {
      policy = DCAwareRoundRobinPolicy.builder()
                                      .withUsedHostsPerRemoteDc(Integer.MAX_VALUE)
                                      .withLocalDc(appConfig.localDc)
                                      .allowRemoteDCsForLocalConsistencyLevel()
                                      .build();
    } else {
      policy = new RoundRobinPolicy();
    }
    if (!appConfig.disableYBLoadBalancingPolicy) {
      policy = new PartitionAwarePolicy(policy);
    }
    return policy;
  }

  /**
   * Helper method to create a Jedis client.
   * @return a Jedis (Redis client) object.
   */
  protected synchronized Jedis getJedisClient() {
    if (jedisClient == null) {
      CmdLineOpts.ContactPoint contactPoint = getRandomContactPoint();
      // Set the timeout to something more than the timeout in the proxy layer.
      jedisClient = new Jedis(contactPoint.getHost(), contactPoint.getPort(),
          appConfig.jedisSocketTimeout);
      redisServerInUse = Arrays.asList(contactPoint);
    }
    return jedisClient;
  }

  /**
   * Helper method to create a YBJedis client.
   * @return a YBJedis (Redis client) object.
   */
  protected synchronized YBJedis getYBJedisClient() {
    if (ybJedisClient == null) {
      Set<HostAndPort> hosts = configuration.getContactPoints().stream()
          .map(cp -> new HostAndPort(cp.getHost(), cp.getPort())).collect(Collectors.toSet());
      // Set the timeout to something more than the timeout in the proxy layer.
      ybJedisClient = new YBJedis(hosts, appConfig.jedisSocketTimeout);
      redisServerInUse = configuration.getContactPoints();
    }
    return ybJedisClient;
  }

  protected synchronized Pipeline getRedisPipeline() {
    if (jedisPipeline == null) {
      jedisPipeline = getJedisClient().pipelined();
    }
    return jedisPipeline;
  }

  protected synchronized JedisCluster getRedisCluster() {
    if (jedisCluster == null) {
      LOG.info("Creating a new jedis cluster");
      Set<HostAndPort> hosts = configuration.getContactPoints().stream()
          .map(cp -> new HostAndPort(cp.getHost(), cp.getPort())).collect(Collectors.toSet());
      jedisCluster = new JedisCluster(hosts);
    }
    return jedisCluster;
  }

  public synchronized void resetClients() {
    jedisClient = null;
    jedisPipeline = null;
    jedisCluster = null;
    redisServerInUse = null;
    cassandra_cluster = null;
    cassandra_session = null;
  }

  Random random = new Random();
  byte[] buffer;
  Checksum checksum = new Adler32();

  // For binary values we store checksum in bytes.
  static final int CHECKSUM_SIZE = 4;
  // For ASCII values we store checksum in hex string.
  static final int CHECKSUM_ASCII_SIZE = CHECKSUM_SIZE * 2;
  // If value size is more than VALUE_SIZE_TO_USE_PREFIX we add prefix in "val: $key" format
  // in order to check if value matches the key during read.
  static final int VALUE_SIZE_TO_USE_PREFIX = 16;
  static final byte ASCII_MARKER = (byte) 'A';
  static final byte BINARY_MARKER = (byte) 'B';

  /////////////////  Helper functions to create or verify a value of given size. ////////////////
  private static boolean isUseChecksum(int valueSize, int checksumSize) {
    return valueSize > checksumSize + 1;
  }

  private static boolean isUsePrefix(int valueSize) {
    return valueSize > VALUE_SIZE_TO_USE_PREFIX;
  }

  protected byte[] getRandomValue(Key key) {
    getRandomValue(key, buffer);
    return buffer;
  }

  protected byte[] getRandomValue(Key key, byte[] outBuffer) {
    getRandomValue(key, outBuffer.length, outBuffer);
    return outBuffer;
  }

  protected void getRandomValue(Key key, int valueSize, byte[] outBuffer) {
	  final byte[] keyValueBytes = key.getValueStr().getBytes();
	  getRandomValue(keyValueBytes, valueSize, outBuffer);
  }

  protected void getRandomValue(byte[] keyValueBytes, int valueSize, byte[] outBuffer) {
    outBuffer[0] = appConfig.restrictValuesToAscii ? ASCII_MARKER : BINARY_MARKER;
    final int checksumSize = appConfig.restrictValuesToAscii ? CHECKSUM_ASCII_SIZE : CHECKSUM_SIZE;
    final boolean isUseChecksum = isUseChecksum(valueSize, checksumSize);
    final int contentSize = valueSize - (isUseChecksum ? checksumSize : 0);
    int i = 1;
    if (isUsePrefix(valueSize)) {

      // Beginning of value is not random, but has format "<MARKER><PREFIX>", where prefix is
      // "val: $key" (or part of it in case small value size). This is needed to verify expected
      // value during read.
      final int prefixSize = Math.min(contentSize - 1 /* marker */, keyValueBytes.length);
      System.arraycopy(keyValueBytes, 0, outBuffer, 1, prefixSize);
      i += prefixSize;
    }

    // Generate randomly the rest of payload leaving space for checksum.
    if (appConfig.restrictValuesToAscii) {
      final int ASCII_START = 32;
      final int ASCII_RANGE_SIZE = 95;
      while (i < contentSize) {
        long r = (long) (random.nextInt() & 0xffffffff);
        // Hack to minimize number of calls to random.nextInt() in order to reduce CPU load.
        // This makes distribution non-uniform, but should be OK for load tests.
        for (int n = Math.min(Integer.BYTES, contentSize - i); n > 0;
          r /= ASCII_RANGE_SIZE, n--) {
          outBuffer[i++] = (byte) (ASCII_START + r % ASCII_RANGE_SIZE);
        }
      }
    } else {
      while (i < contentSize) {
        for (int r = random.nextInt(), n = Math.min(Integer.BYTES, contentSize - i); n > 0;
             r >>= Byte.SIZE, n--)
            outBuffer[i++] = (byte) r;
      }
    }

    if (isUseChecksum) {
      checksum.reset();
      checksum.update(outBuffer, 0, contentSize);
      long cs = checksum.getValue();
      if (appConfig.restrictValuesToAscii) {
        String csHexStr = Long.toHexString(cs);
        // Prepend zeros
        while (i < valueSize - csHexStr.length()) {
          outBuffer[i++] = (byte) '0';
        }
        System.arraycopy(csHexStr.getBytes(), 0, outBuffer, i, csHexStr.length());
      } else {
        while (i < valueSize) {
          outBuffer[i++] = (byte) cs;
          cs >>= Byte.SIZE;
        }
      }
    }
  }

  protected boolean verifyRandomValue(Key key, byte[] value) {
    final boolean isAscii = value[0] == ASCII_MARKER;
    final int checksumSize = isAscii ? CHECKSUM_ASCII_SIZE : CHECKSUM_SIZE;
    final boolean hasChecksum = isUseChecksum(value.length, checksumSize);
    if (isUsePrefix(value.length)) {
      final String keyValueStr = key.getValueStr();
      final int prefixSize = Math.min(keyValueStr.length(), value.length -
                             (hasChecksum ? checksumSize : 0) - 1 /* marker */);
      final String prefix = new String(value, 1, prefixSize);
      // Check prefix.
      if (!prefix.equals(keyValueStr.substring(0, prefixSize))) {
        LOG.fatal("Value mismatch for key: " + key.toString() +
                  ", expected to start with: " + keyValueStr +
                  ", got: " + prefix);
        return false;
      }
    }
    if (hasChecksum) {
      // Verify checksum.
      checksum.reset();
      checksum.update(value, 0, value.length - checksumSize);
      long expectedCs;
      if (isAscii) {
        String csHexStr = new String(value, value.length - checksumSize, checksumSize);
        expectedCs = Long.parseUnsignedLong(csHexStr, 16);
      } else {
        expectedCs = 0;
        for (int i = value.length - 1; i >= value.length - checksumSize; --i) {
          expectedCs <<= Byte.SIZE;
          expectedCs |= (value[i] & 0xFF);
        }
      }
      if (checksum.getValue() != expectedCs) {
        LOG.fatal("Value mismatch for key: " + key.toString() +
                  ", expected checksum: " + expectedCs +
                  ", got: " + checksum.getValue());
        return false;
      }
    }
    return true;
  }

  public void setMainInstance(boolean mainInstance) {
    this.mainInstance = mainInstance;
  }

  ///////////////////// The following methods are overridden by the apps ///////////////////////////

  /**
   * This method is called to allow the app to initialize itself with the various command line
   * options.
   * @param configuration Configuration object for the application.
   */
  public void initialize(CmdLineOpts configuration) {}

  /**
   * The apps extending this base should drop all the tables they create when this method is called.
   * @throws java.lang.Exception in case of DROP statement errors.
   */
  public void dropTable() throws Exception {}

  public void dropCassandraTable(String tableName) {
    String drop_stmt = String.format("DROP TABLE IF EXISTS %s;", tableName);
    getCassandraClient().execute(new SimpleStatement(drop_stmt).setReadTimeoutMillis(60000));
    LOG.info("Dropped Cassandra table " + tableName + " using query: [" + drop_stmt + "]");
  }

  /**
   * The apps extending this base should create all the necessary tables in this method.
   * @param tableOp operation on table, e.g. drop the table at beginning of application launch
   * @throws java.lang.Exception in case of CREATE statement errors.
   */
  public void createTablesIfNeeded(TableOp tableOp) throws Exception {
    LOG.info("Creating YCQL tables...");
    for (String create_stmt : getCreateTableStatements()) {
      Session session = getCassandraClient();
      // consistency level of one to allow cross DC requests.
      session.execute(
        session.prepare(create_stmt)
          .setConsistencyLevel(ConsistencyLevel.ONE)
          .bind());
      LOG.info("Created a Cassandra table using query: [" + create_stmt + "]");
    }
  }

  protected List<String> getCreateTableStatements() {
    return Arrays.asList();
  }

  /**
   * Returns the redis server that we are currently talking to.
   * Used for debug purposes. Returns "" for non-redis workloads.
   * @return String format of redis server that we are talking t.
   */
  public String getRedisServerInUse() {
    if (redisServerInUse == null) {
      return "";
    }
    return redisServerInUse.stream().map(ContactPoint::ToString)
        .collect(Collectors.joining(", "));
  }

  /**
   * This call models an OLTP read for the app to perform read operations.
   * @return Number of reads done, a value of 0 or less indicates no ops were done.
   */
  public long doRead() { return 0; }

  /**
   * This call models an OLTP write for the app to perform write operations.
   * @return Number of writes done, a value of 0 or less indicates no ops were done.
   * @param threadIdx index of thread that invoked this write.
   */
  public long doWrite(int threadIdx) { return 0; }

  /**
   * This call should implement the main logic in non-OLTP apps. Not called for OLTP apps.
   */
  public void run() {}

  /**
   * Default implementation of the status message appender. Override/add to this method any stats
   * that need to be printed to the end user.
   */
  @Override
  public void appendMessage(StringBuilder sb) {
    if (promMetrics != null) {
      sb.append(
          "Num restart read requests: " +
          promMetrics.getCounter("restart_read_requests", getTableName()) +
          " | ");
    }
    sb.append("Uptime: " + (System.currentTimeMillis() - workloadStartTime) + " ms | ");
  }

  /**
   * Report exception.
   *
   * @param e  the exception
   */
  public void reportException(Exception e) {
    LOG.info("Caught Exception: ", e);
    if (e instanceof NoHostAvailableException) {
      NoHostAvailableException ne = (NoHostAvailableException)e;
      for (Map.Entry<EndPoint,Throwable> entry : ne.getErrors().entrySet()) {
        LOG.info("Exception encountered at host " + entry.getKey() + ": ", entry.getValue());
      }
    }
  }

  /**
   * Returns the description of the app.
   * @return the description splitted in lines.
   */
  public List<String> getWorkloadDescription() { return Collections.EMPTY_LIST; }


  /**
   * Returns any workload-specific required command line options.
   * @return the options split in lines.
   */
  public List<String> getWorkloadRequiredArguments() {return Collections.EMPTY_LIST; }

  /**
   * Returns any workload-specific optional command line options.
   * @return the options split in lines.
   */
  public List<String> getWorkloadOptionalArguments() { return Collections.EMPTY_LIST; }

  ////////////// The following methods framework/helper methods for subclasses. ////////////////////

  /**
   * The load tester framework call this method of the base class. This in turn calls the
   * 'initialize()' method which the plugins should implement.
   * @param configuration configuration of the load tester framework.
   * @param enableMetrics Should metrics tracker be enabled.
   */
  public void workloadInit(CmdLineOpts configuration, boolean enableMetrics) {
    workloadStartTime = System.currentTimeMillis();
    this.configuration = configuration;
    initialize(configuration);
    if (enableMetrics) initMetricsTracker();
  }

  public void enableMetrics() {
    initMetricsTracker();
  }

  private synchronized void initMetricsTracker() {
    if (metricsTracker == null) {
      metricsTracker = new MetricsTracker(appConfig.outputJsonMetrics);
      if (appConfig.appType == AppConfig.Type.OLTP) {
        metricsTracker.createMetric(MetricName.Read);
        metricsTracker.createMetric(MetricName.Write);
        metricsTracker.registerStatusMessageAppender(this);
        metricsTracker.start();

        if (appConfig.restartReadsReported) {
          LOG.info("Reporting restart read requests.");
          try {
            promMetrics = new PromMetrics(getNodesAsInet());
          } catch (IOException e) {
            LOG.error("Failed to create prometheus metrics tracker with exception: ", e);
            promMetrics = null;
          }
        }
      }
    }
  }

  /**
   * Helper method to get a random proxy-service contact point to do io against.
   * @return a random proxy-service contact point.
   */
  public ContactPoint getRandomContactPoint() {
    return configuration.getRandomContactPoint();
  }

  /**
   * Returns a list of Inet address objects in the proxy tier. This is needed by Cassandra clients.
   * @return a list of Inet address objects.
   */
  public List<InetSocketAddress> getNodesAsInet() {
    List<InetSocketAddress> inetSocketAddresses = new ArrayList<>();
    for (ContactPoint contactPoint : configuration.getContactPoints()) {
      try {
        for (InetAddress addr : InetAddress.getAllByName(contactPoint.getHost())) {
          inetSocketAddresses.add(new InetSocketAddress(addr, contactPoint.getPort()));
        }
      } catch (UnknownHostException e) {
        throw new IllegalArgumentException("Failed to resolve address: " + contactPoint.getHost(),
            e);
      }
    }
    return inetSocketAddresses;
  }

  /**
   * @return true if the workload has finished running, false otherwise.
   */
  public boolean hasFinished() {
    return hasFinished.get();
  }

  /**
   * Stops the workload running for this app. The purpose of this method is to allow a clean stop
   * of the workload from external inputs.
   */
  public void stopApp() {
    hasFinished.set(true);
  }

  private boolean isOutOfTime() {
    return appConfig.runTimeSeconds > 0 &&
        (System.currentTimeMillis() - workloadStartTime > appConfig.runTimeSeconds * 1000);
  }

  /**
   * Called by the framework to perform write operations - internally measures the time taken to
   * perform the write op and keeps track of the number of keys written, so that we are able to
   * report the metrics to the user.
   * @param threadIdx index of thread that invoked this write.
   */
  public void performWrite(int threadIdx) {
    // If we have written enough keys we are done.
    if (appConfig.numKeysToWrite >= 0 && numKeysWritten.get() >= appConfig.numKeysToWrite
        || isOutOfTime()) {
      hasFinished.set(true);
      return;
    }
    // Perform the write and track the number of successfully written keys.
    long startTs = System.nanoTime();
    long count = doWrite(threadIdx);
    long endTs = System.nanoTime();
    if (count > 0) {
      numKeysWritten.addAndGet(count);
      if (metricsTracker != null) {
        Observation o = new Observation(count, startTs, endTs);
        metricsTracker.getMetric(MetricName.Write).observe(o);
      }
    }
  }

  /**
   * Called by the framework to perform read operations - internally measures the time taken to
   * perform the read op and keeps track of the number of keys read, so that we are able to
   * report the metrics to the user.
   */
  public void performRead() {

    // If we have read enough keys we are done.
    if (appConfig.numKeysToRead >= 0 && numKeysRead.get() >= appConfig.numKeysToRead
        || isOutOfTime()) {
      hasFinished.set(true);
      return;
    }
    // Perform the read and track the number of successfully read keys.
    long startTs = System.nanoTime();
    long count = doRead();
    long endTs = System.nanoTime();
    if (count > 0) {
      numKeysRead.addAndGet(count);
      if (metricsTracker != null) {
        Observation o = new Observation(count, startTs, endTs);
        metricsTracker.getMetric(MetricName.Read).observe(o);
      }
    }
  }

  public void verifyTotalRowsWritten() throws Exception {
    LOG.info("Row count check is not supported for this workload");
  }

  public void recordExistingRowCount() throws Exception {
    LOG.info("Row count check is not supported for this workload");
  }

  public String getTableName() {
    return "AppBaseTable";
  }

  @Override
  public String appenderName() {
    return this.getClass().getSimpleName();
  }

  /**
   * Close the Connection.
   */
  static void close(Connection c) {
    if (c != null) {
      try {
        c.close();
      } catch (Throwable t) {
        LOG.warn("encountered exception closing connection: " + c);
      }
    }
  }

  /**
   * Close the PreparedStatement.
   */
  static void close(PreparedStatement ps) {
    if (ps != null) {
      try {
        ps.close();
      } catch (Throwable t) {
        LOG.warn("encountered exception closing statement: " + t);
      }
    }
  }

  /**
   * Terminate the workload (tear down connections if needed, etc).
   */
  public void terminate() {
    destroyClients();
  }

  protected synchronized void destroyClients() {
    // Only the main app instance should close the shared Cassandra cluster and session and at the
    // end of the workload.
    if (mainInstance && cassandra_session != null) {
      cassandra_session.close();
      cassandra_session = null;
    }
    if (mainInstance && cassandra_cluster != null) {
      cassandra_cluster.close();
      cassandra_cluster = null;
    }
    if (jedisClient != null) {
      jedisClient.close();
    }
  }

  public SimpleLoadGenerator getSimpleLoadGenerator() {
    SimpleLoadGenerator sLoadGeneratorTmp = simpleLoadGenerator;
    if (sLoadGeneratorTmp == null) {
      synchronized (AppBase.class) {
        if (simpleLoadGenerator == null) {
          simpleLoadGenerator = new SimpleLoadGenerator(0,
              appConfig.numUniqueKeysToWrite,
              appConfig.maxWrittenKey);
        }
        sLoadGeneratorTmp = simpleLoadGenerator;
      }
    }
    return sLoadGeneratorTmp;
  }

  public RedisHashLoadGenerator getRedisHashLoadGenerator() {
    RedisHashLoadGenerator redisHashLoadGeneratorTmp = redisHashLoadGenerator;
    if (redisHashLoadGeneratorTmp == null) {
      synchronized (AppBase.class) {
        if (redisHashLoadGenerator == null) {
          redisHashLoadGenerator = new RedisHashLoadGenerator(appConfig.uuid,
              (int)appConfig.numUniqueKeysToWrite, appConfig.numSubkeysPerKey,
              appConfig.keyUpdateFreqZipfExponent, appConfig.subkeyUpdateFreqZipfExponent,
              appConfig.numSubkeysPerWrite, appConfig.numSubkeysPerRead);
        }
        redisHashLoadGeneratorTmp = redisHashLoadGenerator;
      }
    }
    return redisHashLoadGeneratorTmp;
  }

  public static long numOps() {
    return numKeysRead.get() + numKeysWritten.get();
  }

  public static void resetOps() {
    numKeysRead.set(0);
    numKeysWritten.set(0);
  }
}

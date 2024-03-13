package com.yugabyte.sample.apps.anomalies;

import com.yugabyte.sample.common.SimpleLoadGenerator.Key;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;

public class SqlInsertTabletsSkewQuery extends SqlInsertTablets {
  private static final Logger LOG = Logger.getLogger(SqlInsertTabletsSkewQuery.class);

  private static AtomicInteger readSkewSetup = new AtomicInteger(0);
  private static AtomicInteger writeSkewSetup = new AtomicInteger(0);

  private boolean readSkewThread = false;
  private boolean writeSkewThread = false;
  public Thread readSkewThreadInstance = null;
  public Thread writeSkewThreadInstance = null;

  public int writeCounter = 0;
  public boolean writeSkewThreadStarted = false;
  public int readCounter = 0;
  public boolean readSkewThreadStarted = false;

  public void setupReadThread() {
    if (readSkewSetup.get() > 10) return;
    if (readSkewSetup.incrementAndGet() == 1) {
      LOG.info("This is a read skew thread " + " - keys to read: " + appConfig.numKeysToRead);
      long limit = appConfig.numKeysToRead / configuration.getNumReaderThreads() / 2;
      readSkewThread = true;

      readSkewThreadInstance =
          new Thread(
              new Runnable() {
                @Override
                public void run() {
                  LOG.info("Beginning read skew thread up to " + limit + " keys.");
                  SqlInsertTabletsSkewQuery app = new SqlInsertTabletsSkewQuery();
                  for (int i = 0; i < limit; i++) {
                    if (i % 1000 == 0) {
                      LOG.info("Read Skew Iteration: " + i + " / " + limit + " done.");
                    }
                    try {
                      app.doReadNoBarrier(getSimpleLoadGenerator().getKeyToRead());
                    } catch (Exception e) {
                      LOG.info("Failed reading key: ", e);
                    }
                  }
                  LOG.info("Read Skew thread done");
                }
              });
    }
  }

  public void setupWriteThread(int threadIdx) {
    if (writeSkewSetup.get() > 10) return;
    if (writeSkewSetup.incrementAndGet() == 1) {
      LOG.info(
          "This is a write skew thread for "
              + threadIdx
              + " - keys to write: "
              + appConfig.numKeysToWrite);
      long limit = appConfig.numKeysToWrite / configuration.getNumWriterThreads() / 2;
      writeSkewThread = true;

      writeSkewThreadInstance =
          new Thread(
              new Runnable() {
                @Override
                public void run() {
                  LOG.info("Beginning write skew thread with " + limit + " keys.");
                  String th = "Thread" + threadIdx;
                  SqlInsertTabletsSkewQuery app = new SqlInsertTabletsSkewQuery();
                  for (int i = 0; i < limit; i++) {
                    if (i % 1000 == 0) {
                      LOG.info("Write Skew Iteration: " + i + " / " + limit + " done.");
                    }
                    try {
                      Key key = new Key(i, th);
                      app.doWriteNoBarrier(0, key);
                    } catch (Exception e) {
                      LOG.info("Failed writing key: ", e);
                    }
                  }
                  LOG.info("Write Skew thread done");
                }
              });
    }
  }

  @Override
  public long doRead() {
    try {

      long ret = super.doRead();
      readCounter++;
      setupReadThread();

      if (readSkewThread
          && !readSkewThreadStarted
          && readCounter
              >= ((appConfig.numKeysToRead / configuration.getNumReaderThreads()) * 0.3)) {
        readSkewThreadInstance.start();
        readSkewThreadStarted = true;
      }

      return ret;
    } catch (Exception e) {
      LOG.info("Failed reading key: ", e);
      return 0;
    }
  }

  @Override
  public long doWrite(int threadIdx) {
    try {

      long ret = super.doWrite(threadIdx);
      writeCounter++;
      setupWriteThread(threadIdx);

      if (writeSkewThread
          && !writeSkewThreadStarted
          && writeCounter
              >= ((appConfig.numKeysToWrite / configuration.getNumWriterThreads()) * 0.3)) {
        LOG.info("Starting write skew thread");
        writeSkewThreadInstance.start();
        writeSkewThreadStarted = true;
      }

      return ret;
    } catch (Exception e) {
      LOG.info("Failed reading key: ", e);
      return 0;
    }
  }
}

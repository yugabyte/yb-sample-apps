package com.yugabyte.sample.apps.anomalies;

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

  public void setupReadThread() {
    if (readSkewSetup.get() > 10) return;
    if (readSkewSetup.incrementAndGet() == 1) {
      LOG.info("This is a read skew thread");
      readSkewThread = true;

      readSkewThreadInstance =
          new Thread(
              new Runnable() {
                @Override
                public void run() {
                  LOG.info("Beginning read skew thread");
                  for (int i = 0; i < appConfig.numKeysToRead / 2; i++) {
                    if (i % 1000 == 0) {
                      LOG.info("Iteration: " + i + " / " + appConfig.numKeysToRead / 2 + " done.");
                    }
                    try {
                      doReadNoBarrier(
                          getSimpleLoadGenerator().generateKey(-appConfig.numKeysToRead));
                    } catch (Exception e) {
                      LOG.info("Failed reading key: ", e);
                    }
                  }
                }
              });
    }
  }

  public void setupWriteThread(int threadIdx) {
    if (writeSkewSetup.get() > 10) return;
    if (writeSkewSetup.incrementAndGet() == 1) {
      LOG.info("This is a write skew thread");
      writeSkewThread = true;

      writeSkewThreadInstance =
          new Thread(
              new Runnable() {
                @Override
                public void run() {
                  LOG.info("Beginning write skew thread");
                  for (int i = 0; i < appConfig.numKeysToWrite / 2; i++) {
                    if (i % 1000 == 0) {
                      LOG.info("Iteration: " + i + " / " + appConfig.numKeysToWrite / 2 + " done.");
                    }
                    try {
                      doWriteNoBarrier(
                          0, getSimpleLoadGenerator().generateKey(-appConfig.numKeysToWrite));
                    } catch (Exception e) {
                      LOG.info("Failed writing key: ", e);
                    }
                  }
                }
              });
    }
  }

  @Override
  public long doRead() {
    try {

      setupReadThread();
      long ret = super.doRead();

      if (readSkewThread && numKeysRead.get() == appConfig.numKeysToRead * 0.5) {
        readSkewThreadInstance.start();
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

      setupWriteThread(threadIdx);
      long ret = super.doWrite(threadIdx);

      if (writeSkewThread && numKeysWritten.get() == appConfig.numKeysToWrite * 0.5) {
        LOG.info("Starting write skew thread");
        writeSkewThreadInstance.start();
      }

      return ret;
    } catch (Exception e) {
      LOG.info("Failed reading key: ", e);
      return 0;
    }
  }
}

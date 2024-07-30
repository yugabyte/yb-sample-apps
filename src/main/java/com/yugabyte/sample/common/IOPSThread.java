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

package com.yugabyte.sample.common;

import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.yugabyte.sample.apps.AppBase;

/**
 * A class that encapsulates a single IO thread. The thread has an index (which is an integer),
 * models an OLTP app and an IO type (read or write). It performs the required IO as long as
 * the app has not completed all its IO.
 */
public class IOPSThread extends Thread {
  private static final Logger LOG = Logger.getLogger(IOPSThread.class);

  // The thread id.
  protected int threadIdx;

  /**
   * The IO types supported by this class.
   */
  public static enum IOType {
    Write,
    Read,
  }
  // The io type this thread performs.
  IOType ioType;

  // The app that is being run.
  protected AppBase app;

  private int numExceptions = 0;

  private volatile boolean ioThreadFailed = false;

  private final boolean printAllExceptions;

  // Flag to disable concurrency, i.e. execute ops in lock step.
  private final boolean concurrencyDisabled;

  // Lock to disable concurrency.
  // Construct a fair lock to prevent one thread from hogging the lock.
  private static final ReentrantLock lock = new ReentrantLock(true);

  // Throttle reads or writes in this thread.
  private final Throttler throttler;

  public IOPSThread(int threadIdx, AppBase app, IOType ioType, boolean printAllExceptions,
                    boolean concurrencyDisabled, double maxThroughput) {
    this.threadIdx = threadIdx;
    this.app = app;
    this.ioType = ioType;
    this.printAllExceptions = printAllExceptions;
    this.concurrencyDisabled = concurrencyDisabled;
    if (maxThroughput > 0) {
        LOG.info("Throttling " + (ioType == IOType.Read ? "read" : "write") +
                 " ops to " + maxThroughput + " ops/sec.");
        this.throttler = new Throttler(maxThroughput);
    } else {
        this.throttler = null;
    }
  }

  public int getNumExceptions() {
    return numExceptions;
  }

  public boolean hasFailed() {
    return ioThreadFailed;
  }

  public long numOps() {
    return app.numOps();
  }

  /**
   * Cleanly shuts down the IOPSThread.
   */
  public void stopThread() {
    this.app.stopApp();
  }

  /**
   * Method that performs the desired type of IO in the IOPS thread.
   */
  @Override
  public void run() {
    try {
      LOG.debug("Starting " + ioType.toString() + " IOPS thread #" + threadIdx);
      int numConsecutiveExceptions = 0;
      while (!app.hasFinished()) {
        if (concurrencyDisabled) {
          // Wait for the previous thread to execute its step.
          lock.lock();
        }
        try {
          if (throttler != null) {
            throttler.traceOp();
          }
          switch (ioType) {
            case Write: app.performWrite(threadIdx); break;
            case Read: app.performRead(); break;
          }
          numConsecutiveExceptions = 0;
        } catch (RuntimeException e) {
          numExceptions++;
          if (numConsecutiveExceptions++ % 10 == 0 || printAllExceptions) {
            app.reportException(e);
          }
          // Reset state only for redis workload. CQL workloads will hit 'InvalidQueryException'
          // with prepared statements if reset and the same statement is re-executed.
          if (!app.getRedisServerInUse().isEmpty()) {
            LOG.warn("Resetting clients for redis: " + app.getRedisServerInUse());
            app.resetClients();
          }

          if (numConsecutiveExceptions > 500) {
            LOG.error("Had more than " + numConsecutiveExceptions
                      + " consecutive exceptions. Exiting.", e);
            ioThreadFailed = true;
            return;
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {
            LOG.error("Sleep interrupted.", ie);
            ioThreadFailed = true;
            return;
          }
        } finally {
          if (concurrencyDisabled) {
            // Signal the next thread in the wait queue to resume.
            lock.unlock();
          }
          if (throttler != null) {
            // Sleep only after releasing the lock above.
            throttler.throttleOp();
          }
        }
      }
    } finally {
      LOG.debug("IOPS thread #" + threadIdx + " finished");
      app.terminate();
    }
  }
}

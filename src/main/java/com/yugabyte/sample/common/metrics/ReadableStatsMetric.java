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

package com.yugabyte.sample.common.metrics;

import org.apache.log4j.Logger;

public class ReadableStatsMetric {
  private static final Logger LOG = Logger.getLogger(ReadableStatsMetric.class);
  String name;
  private final Object lock = new Object();
  protected long curOpCount = 0;
  protected long curOpLatencyNanos = 0;
  protected long totalOpCount = 0;
  private long lastSnapshotNanos;
  private StatsTracker quantileStats;

  public ReadableStatsMetric(String name) {
    this.name = name;
    lastSnapshotNanos = System.nanoTime();
    quantileStats = new StatsTracker();
  }

  public synchronized void observe(Observation o) {
    accumulate(o.getCount(), o.getLatencyNanos());
    quantileStats.observe(o.getLatencyMillis());
  }

  /**
   * Accumulate metrics with operations processed as one batch.
   *
   * @param numOps number of ops processed as one batch
   * @param batchLatencyNanos whole batch latency
   */
  public synchronized void accumulate(long numOps, long batchLatencyNanos) {
    curOpCount += numOps;
    curOpLatencyNanos += batchLatencyNanos * numOps;
    totalOpCount += numOps;
  }

  public synchronized String getMetricsAndReset() {
    long currNanos = System.nanoTime();
    long elapsedNanos = currNanos - lastSnapshotNanos;
    LOG.debug("currentOpLatency: " + curOpLatencyNanos + ", currentOpCount: " + curOpCount);
    double ops_per_sec = (elapsedNanos == 0) ? 0 : (curOpCount * 1000000000 * 1.0 / elapsedNanos);
    double latency = (curOpCount == 0) ? 0 : (curOpLatencyNanos / 1000000 * 1.0 / curOpCount);
    curOpCount = 0;
    curOpLatencyNanos = 0;
    lastSnapshotNanos = currNanos;
    return String.format(
        "%s: %.2f ops/sec (%.2f ms/op, %.2f ms p99), %d total ops",
        name, ops_per_sec, latency, quantileStats.stats.getPercentile(99), totalOpCount);
  }
}

package com.yugabyte.sample.common;

import org.apache.commons.math3.distribution.PoissonDistribution;

// Throttles the IO operations to a certain throughput.
// The wait time is sampled from a Poisson distribution to introduce
// randomness and prevent every thread from resuming at the same time.
public class Throttler {
    private final PoissonDistribution poissonDistribution;
    private long startTime;

    public Throttler(double maxThroughput) {
        double throttleDelay = 1000.0 / maxThroughput;
        this.poissonDistribution = new PoissonDistribution(throttleDelay);
    }

    // Begin throttling an operation.
    public void traceOp() {
        startTime = System.currentTimeMillis();
    }

    // Operation done. Wait until the next operation can start.
    public void throttleOp() {
        long opDelay = poissonDistribution.sample();
        long endTime = System.currentTimeMillis();
        long waitTime = opDelay - (endTime - startTime);
        if (waitTime > 0) {
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

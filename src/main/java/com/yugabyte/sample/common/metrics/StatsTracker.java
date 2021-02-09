package com.yugabyte.sample.common.metrics;

import com.google.gson.JsonObject;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class StatsTracker {
    private DescriptiveStatistics stats;

    public StatsTracker() {
        this.stats = new DescriptiveStatistics();
    }

    public synchronized void observe(double value) {
        stats.addValue(value);
    }

    public synchronized JsonObject getJson() {
        JsonObject json = new JsonObject();
        json.addProperty("mean", stats.getMean());
        json.addProperty("variance", stats.getVariance());
        json.addProperty("sampleSize", stats.getN());
        json.addProperty("min", stats.getMin());
        json.addProperty("max", stats.getMax());
        json.addProperty("p99", stats.getPercentile(99));
        return json;
    }
}

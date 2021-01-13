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

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.io.Serializable;

public class ComparableStatsMetric implements Serializable {
    private SummaryStatistics latency;
    private ThroughputStats throughput;

    public ComparableStatsMetric() {
        latency = new SummaryStatistics();
        throughput = new ThroughputStats();
    }

    public synchronized void observe(Observation o) {
        throughput.observe(o);
        latency.addValue(o.getLatencyNanos());
    }

    public ComparableStatsData getData() {
        return new ComparableStatsData(latency, throughput.getStats());
    }
}

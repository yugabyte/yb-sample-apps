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

import com.yugabyte.sample.common.metrics.ComparableStatsIO;
import com.yugabyte.sample.common.metrics.ComparableStatsData;
import org.apache.commons.math3.stat.descriptive.StatisticalSummary;
import org.apache.commons.math3.stat.inference.TTest;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CompareStats extends AppBase {

    private static final Logger LOG = Logger.getLogger(CassandraHelloWorld.class);

    /**
     * Run this simple app.
     */
    @Override
    public void run() {
        ComparableStatsData before;
        ComparableStatsData after;
        try {
            ComparableStatsIO beforeReader = new ComparableStatsIO(appConfig.statsDirBefore);
            ComparableStatsIO afterReader = new ComparableStatsIO(appConfig.statsDirAfter);

            before = beforeReader.read(appConfig.statsMetricName);
            after = afterReader.read(appConfig.statsMetricName);
        } catch (Exception e) {
            LOG.fatal("Error reading stats: ", e);
            return;
        }

        TTest test = new TTest();

        StatisticalSummary beforeLatency = before.getLatency();
        StatisticalSummary afterLatency = after.getLatency();
        double pLatency = test.tTest(beforeLatency, afterLatency);
        double liftLatencyNanos = afterLatency.getMean() - beforeLatency.getMean();
        int liftLatencyMillis = (int) (liftLatencyNanos / TimeUnit.MILLISECONDS.toNanos(1));


        StatisticalSummary beforeThroughput = before.getThroughput();
        StatisticalSummary afterThroughput = after.getThroughput();
        double pThroughput = test.tTest(beforeThroughput, afterThroughput);
        int liftThroughputQps = (int) (afterThroughput.getMean() - beforeThroughput.getMean());

        LOG.info("Latency -- A change of " + liftLatencyMillis + " millis with a p-value of " + pLatency);
        LOG.info("Throughput -- A change of " + liftThroughputQps + " ops/sec with a p-value of " + pThroughput);
    }

    static {
        appConfig.appType = AppConfig.Type.Simple;
    }

    @Override
    public List<String> getWorkloadDescription() {
        return Arrays.asList("Compares two different stats outputs using stats.");
    }
}



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

import com.google.gson.JsonObject;

public class Metric {
  private final boolean outputJsonMetrics;

  private final ReadableStatsMetric readableStatsMetric;
  private final JsonStatsMetric jsonStatsMetric;

  public Metric(String name, boolean outputJsonMetrics) {
    this.outputJsonMetrics = outputJsonMetrics;

    readableStatsMetric = new ReadableStatsMetric(name);
    jsonStatsMetric = new JsonStatsMetric(name);
  }

  public synchronized void observe(Observation o) {
    readableStatsMetric.accumulate(o.getCount(), o.getLatencyNanos());
    if (outputJsonMetrics) jsonStatsMetric.observe(o);
  }

  public String getReadableMetricsAndReset() {
    return readableStatsMetric.getMetricsAndReset();
  }

  public JsonObject getJsonMetrics() {
    return jsonStatsMetric.getJson();
  }
}

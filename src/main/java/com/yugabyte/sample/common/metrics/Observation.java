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

import java.util.concurrent.TimeUnit;

public class Observation {
    private long count;
    private long startTsNanos;
    private long endTsNanos;

    public Observation(long count, long startTsNanos, long endTsNanos) {
        this.count = count;
        this.startTsNanos = startTsNanos;
        this.endTsNanos = endTsNanos;
    }

    public long getLatencyNanos() { return endTsNanos - startTsNanos; }

    public double getLatencyMillis() {
        return ((double) getLatencyNanos()) / ((double) TimeUnit.MILLISECONDS.toNanos(1));
    }

    public long getCount() { return count; }

    public long getStartTsNanos() { return startTsNanos; }

    public long getEndTsNanos() { return endTsNanos; }
}

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

public class ThroughputObserver {
    private final long startTsNanos; // inclusive
    private final long endTsNanos; // exclusive
    private long ops;

    ThroughputObserver(long startTsNanos, long endTsNanos) {
        this.startTsNanos = startTsNanos;
        this.endTsNanos = endTsNanos;
        this.ops = 0;
    }

    long observe(Observation o) {
        long toAdd;
        if (o.getEndTsNanos() < startTsNanos || endTsNanos <= o.getStartTsNanos()) {
            toAdd = 0;
        } else {
            toAdd = o.getCount();
        }
        synchronized(this) {
            ops += toAdd;
        }

        return toAdd;
    }

    synchronized long getOps() { return ops; }

    long getStartTsNanos() { return startTsNanos; }

    long getEndTsNanos() { return endTsNanos; }
}

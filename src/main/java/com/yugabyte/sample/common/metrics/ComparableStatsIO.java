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

import java.io.*;

public class ComparableStatsIO {
    private String statsDir;

    public ComparableStatsIO(String statsDir) {
        this.statsDir = statsDir;
    }

    private String getPath(String metricName) {
        return statsDir + "/" + metricName + ".stats.out";
    }

    public ComparableStatsData read(String metricName) throws IOException, ClassNotFoundException {
        InputStream file = new FileInputStream(getPath(metricName));
        ObjectInput input = new ObjectInputStream(file);
        return (ComparableStatsData)input.readObject();
    }

    public void write(String metricName, ComparableStatsData data) {
        try {
            FileOutputStream file = new FileOutputStream(getPath(metricName));
            ObjectOutputStream oos = new ObjectOutputStream(file);
            oos.writeObject(data);
        } catch (Exception e) {
            System.out.println("Exception trying to serialize stats: " + e.toString());
        }
    }
}

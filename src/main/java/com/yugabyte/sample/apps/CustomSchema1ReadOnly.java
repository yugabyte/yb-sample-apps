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

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.log4j.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.yugabyte.sample.common.CmdLineOpts;

public class CustomSchema1ReadOnly extends AppBase {

    private static final Logger LOG = Logger.getLogger(CustomSchema1ReadOnly.class);

    static{

        appConfig.numWriterThreads = 0;
   
        appConfig.numKeysToWrite = -1;
        appConfig.numKeysToRead = -1;
         appConfig.runTimeSeconds = 100;

        // appConfig.numKeysToWrite = 0;
        // appConfig.numKeysToRead = 30;        
        appConfig.skipDDL = true;
       
    }

    // Start customer Id
    private static int start_customer_id = 1;
    // End customer Id
    private static int end_customer_id = 10;


    private static volatile PreparedStatement preparedSelect;

    // Lock for initializing prepared statement objects.
    private static final Object prepareInitLock = new Object();

    private static final String DEFAULT_SELECT_QUERY = "select count(*) from stmnt_reeng.customer_transactions where codacctno in(?) and txndate >= '%s' and txndate < '%s';";
    // private static final String DEFAULT_SELECT_QUERY = " SELECT count(*) from ycsb.usertable ;";

    private static final String DEFAULT_START_DATE = "2023-01-01";
    private static String END_DATE = "2024-01-01";







    @Override
    public void initialize(CmdLineOpts configuration) {
        super.initialize(configuration);
        CommandLine cmd = configuration.getCommandLine();
        if (cmd.hasOption("start_customer_id")) {
         start_customer_id = Integer.parseInt(cmd.getOptionValue("start_customer_id"));
        }
        if (cmd.hasOption("end_customer_id")) {
         end_customer_id = Integer.parseInt(cmd.getOptionValue("end_customer_id"));
        }
        if (cmd.hasOption("num_days_to_read")) {
         int num_days_to_read = Integer.parseInt(cmd.getOptionValue("num_days_to_read"));
         updateEndDare(num_days_to_read);
         
        }
    }


    private void updateEndDare(int num_days_to_read){

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate startDate = LocalDate.parse(DEFAULT_START_DATE, formatter);
        LocalDate newDate = startDate.plusDays(num_days_to_read);
        String newDateStr = newDate.format(formatter);
        END_DATE = newDateStr;

    }


    protected PreparedStatement getPreparedSelect()  {
        PreparedStatement preparedSelectLocal = preparedSelect;
        if (preparedSelectLocal == null) {
            synchronized (prepareInitLock) {
                if (preparedSelect == null) {
                String selectQuery = String.format(DEFAULT_SELECT_QUERY, DEFAULT_START_DATE, END_DATE);
                preparedSelect = getCassandraClient().prepare(selectQuery);
                preparedSelectLocal = preparedSelect;
                }
            }
        }
        return preparedSelectLocal;
    }

    private int getRandomAccountId() {
        return start_customer_id + (int) (Math.random() * (end_customer_id - start_customer_id));
    }

    

    @Override
    public long doRead() {

        String accountId = Integer.toString(getRandomAccountId());

        BoundStatement select = getPreparedSelect().bind(accountId);

        ResultSet rs = getCassandraClient().execute(select);
        List<Row> rows = rs.all();

        if (rows.size() != 1) {
            LOG.fatal("Expected 1 row in result, got " + rows.size());
        }

        // Print the result
        // Row row = rows.get(0);
        // System.out.println("Count: " + row.getLong(0));
    
        return 1;
    }

    @Override
    public List<String> getWorkloadOptionalArguments() {
        return Arrays.asList(
            "--num_reads " + appConfig.numKeysToRead,
            "--num_writes " + appConfig.numKeysToWrite,
            "--value_size " + appConfig.valueSize,
            "--num_threads_read " + appConfig.numReaderThreads,
            "--num_threads_write " + appConfig.numWriterThreads
        );
    }

    
}

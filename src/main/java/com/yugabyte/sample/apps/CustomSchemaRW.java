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

public class CustomSchemaRW extends AppBase {

    private static final Logger LOG = Logger.getLogger(CustomSchemaRW.class);

    static{
        appConfig.readIOPSPercentage = -1;
        appConfig.numWriterThreads = 0;
        appConfig.numReaderThreads = 1;
   
        appConfig.numKeysToWrite = -1;
        appConfig.numKeysToRead = -1;
         appConfig.runTimeSeconds = 100;

        // appConfig.numKeysToWrite = 0;
        // appConfig.numKeysToRead = 30;        
        // appConfig.skipDDL = true;
       
    }

    // Start customer Id
    private static long start_customer_id = 1;
    // End customer Id
    private static long end_customer_id = 10;
    // Static codacctno 
    private static String codacctno = null ;
    // Static codacctno prefix 
    private static String codacctno_prefix = null;

    private static final Object prepareInitLock = new Object();



    private static volatile PreparedStatement preparedSelect;

    private static final String DEFAULT_SELECT_QUERY = "select * from stmnt_reeng.customer_transactions ";
    // private static final String DEFAULT_SELECT_QUERY = " SELECT count(*) from ycsb.usertable ;";
    // where codacctno =  ? and txndate >= '%s' and txndate < '%s';


    private static final String DEFAULT_START_DATE = "2023-01-01";
    private static String END_DATE = null;



    @Override
    public void initialize(CmdLineOpts configuration) {
        super.initialize(configuration);
        CommandLine cmd = configuration.getCommandLine();
        if (cmd.hasOption("start_customer_id")) {
         start_customer_id =  Long.parseLong(cmd.getOptionValue("start_customer_id"));
        }
        if (cmd.hasOption("end_customer_id")) {
         end_customer_id = Long.parseLong(cmd.getOptionValue("end_customer_id"));
        }
        if (cmd.hasOption("num_days_to_read")) {
         int num_days_to_read = Integer.parseInt(cmd.getOptionValue("num_days_to_read"));
         updateEndDare(num_days_to_read);
        }
        if (cmd.hasOption("codacctno")) {
            codacctno = cmd.getOptionValue("codacctno");
        }else if(cmd.hasOption("codacctno_prefix")){
            codacctno_prefix = cmd.getOptionValue("codacctno_prefix");
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
                String selectQuery = null;
                if(codacctno != null){
                    selectQuery = DEFAULT_SELECT_QUERY + String.format(" where codacctno = '%s' ", codacctno);
                }else{
                    selectQuery = DEFAULT_SELECT_QUERY + " where codacctno = ? " ;
                }
                if(END_DATE != null){
                    selectQuery = selectQuery + String.format(" and txndate >= '%s' and txndate < '%s' ", DEFAULT_START_DATE, END_DATE);
                }                
                preparedSelect = getCassandraClient().prepare(selectQuery);
                preparedSelectLocal = preparedSelect;
            }
        }
        return preparedSelectLocal;
    }

    private String getRandomAccountId() {
        long random_id =  start_customer_id + (long) (Math.random() * (end_customer_id - start_customer_id));
        return codacctno_prefix + random_id;
    }

    

    @Override
    public long doRead() {

        BoundStatement select = null;
        if(codacctno != null){
            select = getPreparedSelect().bind();
        }else{
            String accountId = getRandomAccountId();
            select = getPreparedSelect().bind(accountId);
        }
        ResultSet rs = getCassandraClient().execute(select);
        List<Row> rows = rs.all();
        if (rows.size() == 0) {
            LOG.info("No rows found for the query");
        }
       
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

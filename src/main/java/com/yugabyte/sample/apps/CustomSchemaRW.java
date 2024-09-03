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

// import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.math.BigDecimal;


import org.apache.commons.cli.CommandLine;
import org.apache.log4j.Logger;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.yugabyte.sample.common.CmdLineOpts;
import com.datastax.driver.core.LocalDate;


public class CustomSchemaRW extends AppBase {

    private static final Logger LOG = Logger.getLogger(CustomSchemaRW.class);

    static{
        appConfig.readIOPSPercentage = -1;
        appConfig.numWriterThreads = 0;
        appConfig.numReaderThreads = 0;
   
        appConfig.numKeysToWrite = -1;
        appConfig.numKeysToRead = -1;
        appConfig.runTimeSeconds = 100;

        // appConfig.batchSize = 256;

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

    private long currentValue = 0;
    private int divide = 0;



    private static volatile PreparedStatement preparedSelect;
      // The shared prepared statement for inserting into the table.
    private static volatile PreparedStatement preparedInsert;

    private static final String DEFAULT_SELECT_QUERY = "select * from stmnt_reeng.customer_transactions ";
    // private static final String DEFAULT_SELECT_QUERY = " SELECT count(*) from ycsb.usertable ;";
    // where codacctno =  ? and txndate >= '%s' and txndate < '%s';


    private static String start_date = null;
    private static String end_date = null;



    @Override
    public void initialize(CmdLineOpts configuration) {
        super.initialize(configuration);
        CommandLine cmd = configuration.getCommandLine();
        if (cmd.hasOption("start_customer_id")) {
         start_customer_id =  Long.parseLong(cmd.getOptionValue("start_customer_id"));
         currentValue = start_customer_id;
        }
        if (cmd.hasOption("end_customer_id")) {
         end_customer_id = Long.parseLong(cmd.getOptionValue("end_customer_id"));
        }
        
         if (cmd.hasOption("start_date")) {
            start_date = cmd.getOptionValue("start_date");
        }
        if (cmd.hasOption("end_date")) {
            end_date = cmd.getOptionValue("end_date");
        }
        if(cmd.hasOption("num_threads_write")){
            appConfig.numWriterThreads = Integer.parseInt(cmd.getOptionValue("num_threads_write"));  
             divide = (int) ((end_customer_id - start_customer_id) / appConfig.numWriterThreads);
        }
        

        if (cmd.hasOption("codacctno")) {
            codacctno = cmd.getOptionValue("codacctno");
        }else if(cmd.hasOption("codacctno_prefix")){
            codacctno_prefix = cmd.getOptionValue("codacctno_prefix");
        }
    }


    // private void updateEndDare(int num_days_to_read){

    //     DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    //     LocalDate startDate = LocalDate.parse(DEFAULT_START_DATE, formatter);
    //     LocalDate newDate = startDate.plusDays(num_days_to_read);
    //     String newDateStr = newDate.format(formatter);
    //     END_DATE = newDateStr;

    // }


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
                if(start_date != null && end_date != null){
                    selectQuery = selectQuery + String.format(" and txndate >= '%s' and txndate < '%s' ", start_date, end_date);
                } 
                preparedSelect = getCassandraClient().prepare(selectQuery);
                preparedSelectLocal = preparedSelect;
            }
        }
        return preparedSelectLocal;
    }

    private PreparedStatement getPreparedInsert()  {
        PreparedStatement pInsertTmp = preparedInsert;
        if (pInsertTmp == null) {
            synchronized (prepareInitLock) {
                if (preparedInsert == null) {
                // Create the prepared statement object.
                    String insert_stmt = "INSERT INTO  stmnt_reeng.customer_transactions (codacctno, mobilenumber, fname, lname, txndate, random1, random2, random3, random4, random5, random6, random7, random8, random9, random10, random11, random12, random13, random14, random15, random16, random17, random18, random19, random20, reftxnno, codtxnmnemonic, branch, branchcode, branchaddress, coddrcr, refchqno, txndesc, amttxn) VALUES (?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" ;
                    preparedInsert = getCassandraClient().prepare(insert_stmt);
                }
                pInsertTmp = preparedInsert;
            }
        }
        return pInsertTmp;
   }

    private String getRandomAccountId() {
        long random_id =  start_customer_id + (long) (Math.random() * (end_customer_id - start_customer_id));
        return codacctno_prefix + random_id;
    }

    private long nextAccountId(int idx){
        long nextId = currentValue + (idx * divide);
        currentValue++;
        return nextId;
    }


    // random function that generate String between int 1 to 20
    private String random1to20(){
        int random = (int) (Math.random() * 20) + 1;
        return "random" + random;
    }



     @Override
    public long doWrite(int threadIdx) {
        BatchStatement batch = new BatchStatement();
        long numKeysWritten = 1;
        String startDateStr = start_date;
        String endDateStr = end_date;

        long nextId = nextAccountId(threadIdx);
        String accountId = codacctno_prefix + nextId;

        // Define the formatter and parse the dates
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        java.time.LocalDate startDate = java.time.LocalDate.parse(startDateStr, formatter);
        java.time.LocalDate endDate = java.time.LocalDate.parse(endDateStr, formatter);

        for (java.time.LocalDate date = startDate; !date.isAfter(endDate); date = date.plusDays(1)) {
            LocalDate eventDate = LocalDate.fromYearMonthDay(date.getYear(), date.getMonthValue(), date.getDayOfMonth());
            BigDecimal amount = new BigDecimal(100 + (int) (Math.random() * 1000));
            batch.add(getPreparedInsert().bind(
               accountId,
                "+1234567890",
                "First" + accountId,
                "Last" + accountId,
                eventDate,
                random1to20(),
                random1to20(),
                random1to20(),
                random1to20(),
                random1to20(),
                random1to20(),
                random1to20(),
                random1to20(),
                random1to20(),
                random1to20(),
                random1to20(),
                random1to20(),
                random1to20(),
                random1to20(),
                random1to20(),
                random1to20(),
                random1to20(),
                random1to20(),
                random1to20(),
                random1to20(),
                "reftxnno" + random1to20(),
                "codtxnmnemonic",
                "branch",
                "branchcode",
                "branchaddress",
                "coddrcr",
                "refchqno",
                "txndesc",
                amount
            ));
            numKeysWritten++;
           
        }
        ResultSet rs = getCassandraClient().execute(batch);

        return numKeysWritten;
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

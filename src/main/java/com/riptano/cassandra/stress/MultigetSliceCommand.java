package com.riptano.cassandra.stress;

import java.util.concurrent.CountDownLatch;

import org.apache.cassandra.utils.LatencyTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;

public class MultigetSliceCommand extends StressCommand {

    private static Logger log = LoggerFactory.getLogger(MultigetSliceCommand.class);
    
    private final MultigetSliceQuery multigetSliceQuery;
    private final StringSerializer se = StringSerializer.get();
    
    public MultigetSliceCommand(int startKey, CommandArgs commandArgs,
            CountDownLatch countDownLatch) {
        super(startKey, commandArgs, countDownLatch);
        multigetSliceQuery = HFactory.createMultigetSliceQuery(commandArgs.keyspace, se, se);
    }

    @Override
    public Void call() throws Exception {
        int rows = 0;
        multigetSliceQuery.setColumnFamily("Standard1");
        log.debug("Starting MultigetSliceCommand");
        String[] keys = new String[commandArgs.batchSize];
        while (rows < commandArgs.getKeysPerThread()) {
            multigetSliceQuery.setRange("", "", false, commandArgs.columnCount);            
            for (int i = 0; i < commandArgs.batchSize && rows < commandArgs.getKeysPerThread(); i++) {
                keys[i] = String.format("%010d", startKey + rows);                
                rows++;
            }
            multigetSliceQuery.setKeys(keys);
            QueryResult<Rows<String,String>> result = multigetSliceQuery.execute();
            LatencyTracker readCount = Stress.latencies.get(result.getHostUsed());
            readCount.addMicro(result.getExecutionTimeMicro());
            
            log.info("executed multiget batch of {}. {} of {} complete", new Object[]{commandArgs.batchSize, rows, commandArgs.getKeysPerThread()});
        }
        countDownLatch.countDown();
        log.debug("MultigetSliceCommand complete");

        return null;
    }

}

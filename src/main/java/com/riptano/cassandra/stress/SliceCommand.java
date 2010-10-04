package com.riptano.cassandra.stress;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.cassandra.utils.LatencyTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

public class SliceCommand extends StressCommand {
    private static Logger log = LoggerFactory.getLogger(SliceCommand.class);
    
    private final SliceQuery<String, String> sliceQuery;
    
    private static StringSerializer se = StringSerializer.get();
    
    public SliceCommand(int startKey, CommandArgs commandArgs, CountDownLatch countDownLatch) {
        super(startKey, commandArgs, countDownLatch);
        sliceQuery = HFactory.createSliceQuery(commandArgs.keyspace, se, se);
    }

    @Override
    public Void call() throws Exception {
        int rows = 0;
        Random random = new Random();
        sliceQuery.setColumnFamily("Standard1");
        log.info("Starting SliceCommand");
        try {
            while (rows < commandArgs.getKeysPerThread()) {
                sliceQuery.setKey(String.format("%010d", startKey + random.nextInt(commandArgs.getKeysPerThread())));
                sliceQuery.setRange("", "", false, commandArgs.columnCount);
                QueryResult<ColumnSlice<String,String>> result = sliceQuery.execute();            
                LatencyTracker readCount = Stress.latencies.get(result.getHostUsed());
                readCount.addMicro(result.getExecutionTimeMicro());

                rows++;
            }
        } catch (Exception e) {
            log.error("Problem!",e);
        }
        countDownLatch.countDown();
        log.info("SliceCommand complete");
        return null;
    }

}

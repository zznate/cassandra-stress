package com.riptano.cassandra.stress;

import java.util.concurrent.CountDownLatch;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

import org.apache.cassandra.utils.LatencyTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangeSliceCommand extends StressCommand {

    private static Logger log = LoggerFactory.getLogger(RangeSliceCommand.class);
    
    private final RangeSlicesQuery<String, String, String> rangeSlicesQuery;
    private StringSerializer se = StringSerializer.get();
    
    public RangeSliceCommand(int startKey, CommandArgs commandArgs,
            CountDownLatch countDownLatch) {
        super(startKey, commandArgs, countDownLatch);
        rangeSlicesQuery = HFactory.createRangeSlicesQuery(commandArgs.keyspace, se, se, se);
    }

    @Override
    public Void call() throws Exception {
        int rows = 0;
        rangeSlicesQuery.setColumnFamily("Standard1");
        log.debug("Starting SliceCommand");
        while (rows < commandArgs.getKeysPerThread()) {
            for (int i = 0; i < commandArgs.batchSize; i++) {
                rangeSlicesQuery.setKeys(String.format("%010d", startKey), String.format("%010d",startKey + commandArgs.getKeysPerThread()));
                rangeSlicesQuery.setRange(null, null, false, commandArgs.columnCount);
                QueryResult<OrderedRows<String,String,String>> result = rangeSlicesQuery.execute();
                LatencyTracker readCount = Stress.latencies.get(result.getHostUsed());
                readCount.addMicro(result.getExecutionTimeMicro());
                rows++;
            }
            log.info("executed batch of {}. {} of {} complete", new Object[]{commandArgs.batchSize, rows, commandArgs.getKeysPerThread()});
        }
        countDownLatch.countDown();
        log.debug("SliceCommand complete");
        return null;
    }

}
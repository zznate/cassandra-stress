package com.riptano.cassandra.stress;

import java.util.Random;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.HColumnFamilyImpl;
import me.prettyprint.hector.api.HColumnFamily;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SliceCommand extends StressCommand {
    private static Logger log = LoggerFactory.getLogger(SliceCommand.class);
    
    private final SliceQuery<String, String, String> sliceQuery;
    // TODO replace with CFT!
    private final HColumnFamily<String, String> columnFamily;
    
    private static StringSerializer se = StringSerializer.get();
    
    public SliceCommand(int startKey, CommandArgs commandArgs, CommandRunner commandRunner) {
        super(startKey, commandArgs, commandRunner);
        sliceQuery = HFactory.createSliceQuery(commandArgs.keyspace, se, se, se);
        columnFamily = new HColumnFamilyImpl<String, String>(commandArgs.keyspace, commandArgs.workingColumnFamily, se, se);
        columnFamily.setCount(commandArgs.columnCount);
    }

    @Override
    public Void call() throws Exception {
        int rows = 0;
        Random random = new Random();
        sliceQuery.setColumnFamily(commandArgs.workingColumnFamily);
        log.debug("Starting SliceCommand");
        try {            
            while (rows < commandArgs.getKeysPerThread()) {
                long nanos = System.nanoTime();
                columnFamily.addKey(String.format("%010d", startKey + random.nextInt(commandArgs.getKeysPerThread())));
                //sliceQuery.setKey(String.format("%010d", startKey + random.nextInt(commandArgs.getKeysPerThread())));
                //sliceQuery.setRange(null, null, false, commandArgs.columnCount);
                //QueryResult<ColumnSlice<String,String>> result = sliceQuery.execute();
                columnFamily.getColumns();
                //LatencyTracker readCount = commandRunner.latencies.get(new CassandraHost("localhost:9160"));
                ///readCount.addMicro((System.nanoTime() - nanos) / 1000);
                columnFamily.removeKeys().clear();                
                rows++;
            }
        } catch (Exception e) {
            log.error("Problem: ", e);
        }
        commandRunner.doneSignal.countDown();
        log.debug("SliceCommand complete");
        return null;
    }

}

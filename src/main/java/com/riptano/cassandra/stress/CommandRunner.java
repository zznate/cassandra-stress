package com.riptano.cassandra.stress;

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import me.prettyprint.cassandra.service.CassandraHost;

import org.apache.cassandra.utils.LatencyTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Segments the execution from the command implementation. Also holds
 * the CountDownLatch and latency tracker structures for tracking 
 * progress and statistics
 *  
 * @author zznate <nate@riptano.com>
 */
public class CommandRunner {
    
    private static final Logger log = LoggerFactory.getLogger(CommandRunner.class);
    
    final Map<CassandraHost, LatencyTracker> latencies;
    CountDownLatch doneSignal;
    private Operation previousOperation;
    
    public CommandRunner(Set<CassandraHost> cassandraHosts) {
        latencies = new ConcurrentHashMap<CassandraHost, LatencyTracker>();
        for (CassandraHost host : cassandraHosts) {
            latencies.put(host, new LatencyTracker());
        }        
    }
    
    
    public void processCommand(CommandArgs commandArgs) throws Exception {
        if ( commandArgs.getOperation() != Operation.REPLAY ) {
            previousOperation = commandArgs.getOperation();
        }
        
        ExecutorService exec = Executors.newFixedThreadPool(commandArgs.threads);
        log.info("Starting command run at {}", new Date());
        long currentTime = System.currentTimeMillis();
        for (int execCount = 0; execCount < commandArgs.getExecutionCount(); execCount++) {
            doneSignal = new CountDownLatch(commandArgs.threads);
            for (int i = 0; i < commandArgs.threads; i++) {
                log.info("submitting task {}", i+1);
                exec.submit(getCommandInstance(i*commandArgs.getKeysPerThread(), commandArgs, this));
            }
            log.info("all tasks submitted for execution for execution {} of {}", execCount+1, commandArgs.getExecutionCount());
            doneSignal.await();    
        }
        log.info("Finished command run at {} total duration: {} seconds", new Date(), (System.currentTimeMillis()-currentTime)/1000);
        
        
        exec.shutdown();
        
        for (CassandraHost host : latencies.keySet()) {
            LatencyTracker latency = latencies.get(host);
            log.info("Latency for host {}:\n Op Count {} \nRecentLatencyHistogram {} \nRecent Latency Micros {} \nTotalLatencyHistogram {} \nTotalLatencyMicros {}", 
                    new Object[]{host.getName(), latency.getOpCount(), latency.getRecentLatencyHistogramMicros(), latency.getRecentLatencyMicros(),
                    latency.getTotalLatencyHistogramMicros(), latency.getTotalLatencyMicros()});
        }        
    }
    
    private StressCommand getCommandInstance(int startKeyArg, CommandArgs commandArgs, CommandRunner commandRunner) {
        
        int startKey = commandArgs.startKey + startKeyArg;
        if ( log.isDebugEnabled() ) {
          log.debug("Command requested with starting key pos {} and op {}", startKey, commandArgs.getOperation());
        }
        
        Operation operation = commandArgs.getOperation();
        if ( operation.equals(Operation.REPLAY )) {
            operation = previousOperation;
        }
        switch(operation) {
        case INSERT:
            return new InsertCommand(startKey, commandArgs, commandRunner);        
        case READ:
            return new SliceCommand(startKey, commandArgs, commandRunner);     
        case RANGESLICE:
            return new RangeSliceCommand(startKey, commandArgs, commandRunner);
        case MULTIGET:
            return new MultigetSliceCommand(startKey, commandArgs, commandRunner);
        case VERIFY_LAST_INSERT:
          return new VerifyLastInsertCommand(startKey, commandArgs, commandRunner);
        case COUNTERSPREAD:
          return new BucketingCounterSpreadCommand(startKey, commandArgs, commandRunner);
        };
        return new InsertCommand(startKey, commandArgs, commandRunner);
    }

}

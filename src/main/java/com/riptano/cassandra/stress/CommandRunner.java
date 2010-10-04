package com.riptano.cassandra.stress;

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

public class CommandRunner {
    
    private static final Logger log = LoggerFactory.getLogger(CommandRunner.class);
    
    final Map<CassandraHost, LatencyTracker> latencies;
    CountDownLatch doneSignal;
    
    public CommandRunner(Set<CassandraHost> cassandraHosts) {
        latencies = new ConcurrentHashMap<CassandraHost, LatencyTracker>();        
        for (CassandraHost host : cassandraHosts) {
            latencies.put(host, new LatencyTracker());
        }        
    }
    
    
    public void processCommand(CommandArgs commandArgs) throws Exception {               
                       
        doneSignal = new CountDownLatch(commandArgs.clients);
        ExecutorService exec = Executors.newFixedThreadPool(commandArgs.clients);
        for (int i = 0; i < commandArgs.clients; i++) {            
            exec.submit(CommandFactory.getInstance(i*commandArgs.getKeysPerThread(), commandArgs, this));
        }
        log.info("all tasks submitted for execution...");
        doneSignal.await();
        exec.shutdown();
        for (CassandraHost host : latencies.keySet()) {
            LatencyTracker latency = latencies.get(host);
            log.info("Latency for host {}:\n Op Count {} \nRecentLatencyHistogram {} \nRecent Latency Micros {} \nTotalLatencyHistogram {} \nTotalLatencyMicros {}", 
                    new Object[]{host.getName(), latency.getOpCount(), latency.getRecentLatencyHistogramMicros(), latency.getRecentLatencyMicros(),
                    latency.getTotalLatencyHistogramMicros(), latency.getTotalLatencyMicros()});
        }        
    }

}

package com.riptano.cassandra.stress;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.cassandra.utils.LatencyTracker;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initiate a stress run against an Apache Cassandra cluster
 *
 * @author zznate <nate@riptano.com>
 */
public class Stress {
    
    private static Logger log = LoggerFactory.getLogger(Stress.class);       
    
    static Map<CassandraHost, LatencyTracker> latencies;
    
    public static void main( String[] args ) throws Exception 
    {

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse( buildOptions(), args);
        if ( cmd.hasOption('h') || cmd.hasOption("help")) {
            printHelp();
            System.exit(0);
        }
        CommandArgs commandArgs = new CommandArgs();
        

        if (cmd.hasOption("threads")) {
            commandArgs.clients = getIntValueOrExit(cmd, "threads");
        }
        
                    
        String seedHost = cmd.getArgList().size() > 0 ? cmd.getArgs()[0] : "localhost:9160";
        
        log.info("Starting stress run using seed {} for {} clients...", seedHost, commandArgs.clients);
        
    
        if ( cmd.hasOption("num-keys") ) {
            commandArgs.rowCount = getIntValueOrExit(cmd, "num-keys");
        }
        log.error("comArgs: " + commandArgs.rowCount);

        if ( cmd.hasOption("batch-size")) {
            commandArgs.batchSize = getIntValueOrExit(cmd, "batch-size");
        }

        if ( cmd.hasOption("columns")) {
            commandArgs.columnCount = getIntValueOrExit(cmd, "columns");
        }
        
        if (cmd.hasOption("operation")) {
            commandArgs.operation = cmd.getOptionValue("operation");
        }
        
        int keysPerThread = commandArgs.getKeysPerThread();
        
        log.info("{} {} columns into {} keys in batches of {} from {} threads",
                new Object[]{commandArgs.operation, commandArgs.columnCount, commandArgs.rowCount, 
                commandArgs.batchSize, commandArgs.clients});
        
        CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator(seedHost);
        latencies = new ConcurrentHashMap<CassandraHost, LatencyTracker>();
        Cluster cluster = HFactory.createCluster("StressCluster", cassandraHostConfigurator);
        log.info("Retrieved cluster name from host {}",cluster.describeClusterName());
        for (CassandraHost host : cluster.getKnownPoolHosts(true)) {
            latencies.put(host, new LatencyTracker());
        }
        
        commandArgs.keyspace = HFactory.createKeyspace("Keyspace1", cluster);
        
        CountDownLatch doneSignal = new CountDownLatch(commandArgs.clients);
        ExecutorService exec = Executors.newFixedThreadPool(commandArgs.clients);
        for (int i = 0; i < commandArgs.clients; i++) {            
            exec.submit(CommandFactory.getInstance(i*keysPerThread, commandArgs, doneSignal));
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
    
    // TODO if --use-all-hosts, then buildHostsFromRing()
    // treat the host as a single arg, init cluster and call addHosts for the ring
    
    
    private static Options buildOptions() {
        Options options = new Options();
        options.addOption("h", "help", false, "Print this help message and exit");
        options.addOption("t","threads", true, "The number of client threads we will create");
        options.addOption("n","num-keys",true,"The number of keys to create");
        options.addOption("c","columns",true,"The number of columsn to create per key");
        options.addOption("b","batch-size",true,"The number of rows in the batch_mutate call");
        options.addOption("o","operation",true,"One of insert, read, rangeslice, multiget");
        return options;
    }
    
    private static void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "stress [options]... url1,[[url2],[url3],...]", buildOptions() );
    }
    
    private static int getIntValueOrExit(CommandLine cmd, String optionVal) {
        try {
            return Integer.valueOf(cmd.getOptionValue(optionVal));            
        } catch (NumberFormatException ne) {
            log.error("Invalid number of {} provided - must be a reasonably sized positive integer", optionVal);
            System.exit(0);
        }         
        return 0;
    }
}

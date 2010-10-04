package com.riptano.cassandra.stress;

import jline.ConsoleReader;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
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
    
    private CommandArgs commandArgs;
    private CommandRunner commandRunner;
    
    
    public static void main( String[] args ) throws Exception {
        Stress stress = new Stress();
        stress.processArgs(args);
        
        ConsoleReader reader = new ConsoleReader();
        String line;
        while ((line = reader.readLine("[cassandra-stress] ")) != null) {
            if ( line.equalsIgnoreCase("exit")) {
                System.exit(0);
            }
            stress.processCommand(reader, line);
        }
    }
    
    private void processCommand(ConsoleReader reader, String line) throws Exception {
        // TODO catch command error(s) here, simply errmsg handoff to stdin loop above
        
        commandArgs.operation = line;
        if ( commandArgs.validateCommand() ) {
            commandRunner.processCommand(commandArgs);    
        } else {
            reader.printString("Invalid command. Must be one of: read, rangeslice, multiget\n");
        }        
    }
    
    
    private void processArgs(String[] args) throws Exception {
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse( buildOptions(), args);
        if ( cmd.hasOption("help")) {
            printHelp();
            System.exit(0);
        }
        if ( commandArgs == null ) {
            commandArgs = new CommandArgs();
        }
        

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
        
        log.info("{} {} columns into {} keys in batches of {} from {} threads",
                new Object[]{commandArgs.operation, commandArgs.columnCount, commandArgs.rowCount, 
                commandArgs.batchSize, commandArgs.clients});
        
        CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator(seedHost);
        if ( cmd.hasOption("unframed")) {
            cassandraHostConfigurator.setUseThriftFramedTransport(false);
        }
        
        
        Cluster cluster = HFactory.createCluster("StressCluster", cassandraHostConfigurator);
        
        commandArgs.keyspace = HFactory.createKeyspace("Keyspace1", cluster);
        commandRunner = new CommandRunner(cluster.getKnownPoolHosts(true));
        commandRunner.processCommand(commandArgs);
        
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
        options.addOption("m","unframed",false,"Disable use of TFramedTransport");
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

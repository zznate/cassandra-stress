package com.riptano.cassandra.stress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CommandFactory {
  
    private static Logger log = LoggerFactory.getLogger(CommandFactory.class);

    public static StressCommand getInstance(int startKey, CommandArgs commandArgs, CommandRunner commandRunner) {
        switch(commandArgs.getOperation()) {
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
        };
        log.info("Runnig default Insert command...");
        return new InsertCommand(startKey, commandArgs, commandRunner);
    }
}

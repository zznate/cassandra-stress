package com.riptano.cassandra.stress;


public class CommandFactory {

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
        };
        return new InsertCommand(startKey, commandArgs, commandRunner);
    }
}

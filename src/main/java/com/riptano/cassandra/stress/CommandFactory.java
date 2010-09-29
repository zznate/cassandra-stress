package com.riptano.cassandra.stress;

import java.util.concurrent.CountDownLatch;

public class CommandFactory {

    public static StressCommand getInstance(int startKey, CommandArgs commandArgs, CountDownLatch countDownLatch) {
        switch(commandArgs.getOperation()) {
        case INSERT:
            return new InsertCommand(startKey, commandArgs, countDownLatch);        
        case READ:
            return new SliceCommand(startKey, commandArgs, countDownLatch);     
        case RANGESLICE:
            return new RangeSliceCommand(startKey, commandArgs, countDownLatch);
        case MULTIGET:
            // TODO
        };
        return new InsertCommand(startKey, commandArgs, countDownLatch);
    }
}

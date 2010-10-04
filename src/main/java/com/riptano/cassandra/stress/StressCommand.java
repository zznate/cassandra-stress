package com.riptano.cassandra.stress;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.apache.cassandra.utils.LatencyTracker;

import me.prettyprint.hector.api.Keyspace;

public abstract class StressCommand implements Callable<Void> { 
    
    protected final CommandArgs commandArgs;
    protected final int startKey;
    protected final CommandRunner commandRunner;
    
    public StressCommand(int startKey, CommandArgs commandArgs, CommandRunner commandRunner) {
        this.commandArgs = commandArgs;
        this.startKey = startKey;        
        this.commandRunner = commandRunner;
    }

}

package com.riptano.cassandra.stress;

import me.prettyprint.hector.api.Keyspace;

public class CommandArgs {

    public Keyspace keyspace;    
    public int rowCount = DEF_INSERT_COUNT; 
    public int columnCount = DEF_COLUMN_COUNT; 
    public int columnWidth = DEF_COLUMN_WIDTH;
    public int batchSize = DEF_BATCH_SIZE;
    public String operation = DEF_OPERATION;
    public int threads = DEF_CLIENTS;
    public int clients = DEF_CLIENTS;
    public int replayCount = DEF_REPLAY_COUNT;
    public int startKey = DEF_START_KEY;
    public String workingKeyspace = DEF_KEYSPACE;
    public String workingColumnFamily = DEF_COLUMN_FAMILY;
    
    private static int DEF_CLIENTS = 50;
    private static int DEF_INSERT_COUNT = 10000;
    private static int DEF_BATCH_SIZE = 100;
    private static int DEF_COLUMN_COUNT = 10;
    private static String DEF_OPERATION = "insert";
    private static int DEF_REPLAY_COUNT = 0;
    private static int DEF_COLUMN_WIDTH = 16;
    private static int DEF_START_KEY = 0;
    private static String DEF_KEYSPACE = "StressKeyspace";
    private static String DEF_COLUMN_FAMILY = "StressStandard";
    
    public int getKeysPerThread() {
        // TODO check if batchSize is greater than this, reset if so
        return rowCount / threads;
    }
    
    public int getExecutionCount() {
        return replayCount == DEF_REPLAY_COUNT ? 1 : replayCount;
    }
    
    public boolean validateCommand() {
        try {
            getOperation();
        } catch (IllegalArgumentException e) {
            return false;
        }
        return true;
    }
    
    public Operation getOperation() {        
        return Operation.get(operation);
    }
    
    
}

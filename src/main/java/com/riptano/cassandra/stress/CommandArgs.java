package com.riptano.cassandra.stress;

import me.prettyprint.hector.api.Keyspace;

public class CommandArgs {

    public Keyspace keyspace;    
    public int rowCount = DEF_INSERT_COUNT; 
    public int columnCount = DEF_COLUMN_COUNT; 
    public int batchSize = DEF_BATCH_SIZE;
    public String operation = DEF_OPERATION;
    public int clients = DEF_CLIENTS;   
    
    private static int DEF_CLIENTS = 50;
    private static int DEF_INSERT_COUNT = 10000;
    private static int DEF_BATCH_SIZE = 100;
    private static int DEF_COLUMN_COUNT = 10;
    private static String DEF_OPERATION = "insert";
    
    public int getKeysPerThread() {
        // TODO check if batchSize is greater than this, reset if so
        return rowCount / clients;
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

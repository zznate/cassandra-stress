package com.riptano.cassandra.stress;

public enum Operation {
    INSERT("insert"),
    READ("read"),
    RANGESLICE("rangeslice"),
    MULTIGET("multiget");
    
    private final String op;
    
    Operation(String val) {
        this.op = val;
    }
    
    public static Operation get(String op) {
        return Operation.valueOf(op.toUpperCase());
    }
}

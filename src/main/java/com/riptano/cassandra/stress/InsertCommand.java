package com.riptano.cassandra.stress;

import java.util.concurrent.CountDownLatch;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.cassandra.utils.LatencyTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsertCommand extends StressCommand {

    private static Logger log = LoggerFactory.getLogger(InsertCommand.class);
    
    protected final Mutator mutator;
    
    public InsertCommand(int startKey, CommandArgs commandArgs, CommandRunner commandRunner) {
        super(startKey, commandArgs, commandRunner);
        mutator = HFactory.createMutator(commandArgs.keyspace);
    }

    @Override
    public Void call() throws Exception {

        String key = null;
        int rows = 0;
        log.info("StartKey: {} for thread {}", startKey, Thread.currentThread().getId());
        while (rows < commandArgs.getKeysPerThread()) {
            for (int j = 0; j < commandArgs.batchSize && rows < commandArgs.getKeysPerThread(); j++) {
                key = String.format("%010d", rows+startKey);
                for (int j2 = 0; j2 < commandArgs.columnCount; j2++) {
                    mutator.addInsertion(key, "Standard1", HFactory.createStringColumn("c"+j2, "value_"+j2));                    
                }
                rows++;
            }
            
            MutationResult mr = mutator.execute();
            LatencyTracker writeCount = commandRunner.latencies.get(mr.getHostUsed());
            writeCount.addMicro(mr.getExecutionTimeMicro());
            mutator.discardPendingMutations();
            log.info("executed batch of {}. {} of {} complete", new Object[]{commandArgs.batchSize, rows, commandArgs.getKeysPerThread()});
        }
        commandRunner.doneSignal.countDown();
        log.info("Last key was: {} for thread {}", key, Thread.currentThread().getId());
        // while less than mutationBatchSize,
        // - while less than rowCount
        //   - mutator.insert
        // mutator.execute();
        
        
        log.info("Executed chunk of {}. Latch now at {}", commandArgs.getKeysPerThread(), commandRunner.doneSignal.getCount());
        return null;
    }
    
    
}

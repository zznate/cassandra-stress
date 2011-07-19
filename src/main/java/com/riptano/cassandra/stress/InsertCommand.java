package com.riptano.cassandra.stress;

import java.util.concurrent.CountDownLatch;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.cassandra.utils.LatencyTracker;
import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsertCommand extends StressCommand {

    private static final String KEY_FORMAT = "%010d";

    private static Logger log = LoggerFactory.getLogger(InsertCommand.class);
    
    protected final Mutator<String> mutator;
    
    public InsertCommand(int startKey, CommandArgs commandArgs, CommandRunner commandRunner) {
        super(startKey, commandArgs, commandRunner);
        mutator = HFactory.createMutator(commandArgs.keyspace, StringSerializer.get());
    }

    @Override
    public Void call() throws Exception {

        String key = null;
        // take into account string formatting for column width
        int colWidth = commandArgs.columnWidth - 9 <= 0 ? 7 : commandArgs.columnWidth -9;  
        int rows = 0;        
        log.info("StartKey: {} for thread {}", startKey, Thread.currentThread().getId());
        while (rows < commandArgs.getKeysPerThread()) {
            if ( log.isDebugEnabled() ) {
                log.debug("rows at: {} for thread {}", rows, Thread.currentThread().getId());
            }
            for (int j = 0; j < commandArgs.batchSize; j++) {
                key = String.format(KEY_FORMAT, rows+startKey);
                for (int j2 = 0; j2 < commandArgs.columnCount; j2++) {
                    mutator.addInsertion(key, commandArgs.workingColumnFamily, HFactory.createStringColumn(String.format(COLUMN_NAME_FORMAT, j2),
                            String.format(COLUMN_VAL_FORMAT, j2, RandomStringUtils.random(colWidth))));     
                    if ( j2 > 0 && j2 % commandArgs.batchSize == 0 ) {
                      executeMutator(rows);
                    }
                }
                
                if (++rows == commandArgs.getKeysPerThread() ) {
                    break;
                }
                
            }
            executeMutator(rows);
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

    private void executeMutator(int rows) {
      try {
          MutationResult mr = mutator.execute();
          // could be null here when our batch size is zero
          if ( mr.getHostUsed() != null ) {
            LatencyTracker writeCount = commandRunner.latencies.get(mr.getHostUsed());
            if ( writeCount != null )
              writeCount.addMicro(mr.getExecutionTimeMicro());
          }
          mutator.discardPendingMutations();

          log.info("executed batch of {}. {} of {} complete", new Object[]{commandArgs.batchSize, rows, commandArgs.getKeysPerThread()});

      } catch (Exception ex){
          log.error("Problem executing insert:",ex);
      }
    }
    
    private static final String COLUMN_VAL_FORMAT = "%08d_%s";
    private static final String COLUMN_NAME_FORMAT = "col_%08d";
}

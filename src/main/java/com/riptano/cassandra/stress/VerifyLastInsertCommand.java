package com.riptano.cassandra.stress;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.cassandra.utils.LatencyTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single threaded operation. 
 * Writes an integer, and read it back to verify it exists.
 * 
 * Read/Write CL = Quorum.
 * 
 * @author patricioe (Patricio Echague - patricio@datastax.com)
 *
 */
public class VerifyLastInsertCommand extends StressCommand {

    private static Logger log = LoggerFactory.getLogger(VerifyLastInsertCommand.class);

    protected final Mutator<String> mutator;
    private final SliceQuery<String, String, String> sliceQuery;
    private StringSerializer se = StringSerializer.get();
    
    public VerifyLastInsertCommand(int startKey, CommandArgs commandArgs, CommandRunner commandRunner) {
        super(startKey, commandArgs, commandRunner);
        verifyCondition();
        mutator = HFactory.createMutator(commandArgs.keyspace, StringSerializer.get());
        sliceQuery = HFactory.createSliceQuery(commandArgs.keyspace, se, se, se);
    }

    private void verifyCondition() {
      if (commandArgs.threads != 1) {
        log.error("VerifyLastInsertCommand should run single threaded. \"-t 1\"");
        throw new RuntimeException("Total threads should be 1.");
      }
    }

    @Override
    public Void call() throws Exception {
      log.debug("Starting VerifyLastInsertCommand");
      String key = "test";
      sliceQuery.setColumnFamily(commandArgs.workingColumnFamily);

      log.info("StartKey: {} for thread {}", key, Thread.currentThread().getId());
      String colValue;

      for (int col = 0; col < commandArgs.columnCount; col++) {
        colValue = String.format(COLUMN_VAL_FORMAT, col);
        mutator.addInsertion(key, 
                             commandArgs.workingColumnFamily, 
                             HFactory.createStringColumn(String.format(COLUMN_NAME_FORMAT, col),
                                 colValue));
        executeMutator(col);

        // Let's verify
        sliceQuery.setKey(key);
        sliceQuery.setRange(null, null, true, 1);
        QueryResult<ColumnSlice<String,String>> result = sliceQuery.execute();
        String actualValue = result.get().getColumns().get(0).getValue();

        if (!actualValue.equals(colValue)) {
          log.error("Column values don't match. Expected: " + colValue + " - Actual: " + actualValue);
          break;
        }
      }

      commandRunner.doneSignal.countDown();
      log.debug("VerifyLastInsertCommand complete");
      return null;
    }

    private void executeMutator(int cols) {
      try {
          MutationResult mr = mutator.execute();
          // could be null here when our batch size is zero
          if ( mr.getHostUsed() != null ) {
            LatencyTracker writeCount = commandRunner.latencies.get(mr.getHostUsed());
            if ( writeCount != null )
              writeCount.addMicro(mr.getExecutionTimeMicro());
          }
          mutator.discardPendingMutations();

          log.info("executed batch of {}. {} of {} complete", new Object[]{commandArgs.batchSize, cols, commandArgs.getKeysPerThread()});

      } catch (Exception ex){
          log.error("Problem executing insert:",ex);
      }
    }
    
    private static final String COLUMN_VAL_FORMAT = "%08d";
    private static final String COLUMN_NAME_FORMAT = "%08d";
}

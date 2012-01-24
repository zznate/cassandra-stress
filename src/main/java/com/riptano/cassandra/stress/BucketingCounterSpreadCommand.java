package com.riptano.cassandra.stress;

import java.util.TimeZone;

import me.prettyprint.cassandra.model.HCounterColumnImpl;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.BatchSizeHint;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testing a scenario where HH *might* cause counter replication to loop
 * on moderately sized clusters
 *
 * @author zznate
 */
public class BucketingCounterSpreadCommand extends StressCommand {

  private static Logger log = LoggerFactory.getLogger(BucketingCounterSpreadCommand.class);

  private static final long MINS_IN_YEAR = 525600L;
  private static final long MINS_IN_MONTH = 43800L;
  // more rounded version from 60 mins/hour * 7 days *
  private static final long MINS_IN_WEEK = 10080L;
  private static final long MINS_IN_DAY = 1440L;
  private static final long MINS_IN_HOUR = 60L;

  public BucketingCounterSpreadCommand(int startKey, CommandArgs commandArgs, CommandRunner commandRunner) {
    super(startKey, commandArgs, commandRunner);
  }

  @Override
  public Void call() throws Exception {
    log.info("In call on counter insert");
    Mutator<String> counterMutator =
      HFactory.createMutator(commandArgs.keyspace, StringSerializer.get(), new BatchSizeHint(500,2));
    int rows = 0;
    for (;rows < commandArgs.getKeysPerThread(); rows++) {

      log.info("current count {}", rows);
      // build all bucket - 1 row
      new CounterColumnBuilder(BucketType.ALL,rows)
        .applyClicks(1)
        .applyView(1)
        .addToMutation(counterMutator);

      // 12 rows
      new CounterColumnBuilder(BucketType.MONTH,rows)
        .applyClicks(1)
        .applyView(1)
        .addToMutation(counterMutator);

      // 52 rows
      new CounterColumnBuilder(BucketType.WEEK,rows)
        .applyClicks(1)
        .applyView(1)
        .addToMutation(counterMutator);

      // 365 rows
      new CounterColumnBuilder(BucketType.DAY,rows)
        .applyClicks(1)
        .applyView(1)
        .addToMutation(counterMutator);

      // 8760 rows
      new CounterColumnBuilder(BucketType.HOUR,rows)
        .applyClicks(1)
        .applyView(1)
        .addToMutation(counterMutator);

      // 525600 rows
      new CounterColumnBuilder(BucketType.MINUTE,rows)
        .applyClicks(1)
        .applyView(1)
        .addToMutation(counterMutator);

      // TODO test≈ auto-batching here
      if ( rows % 500 == 0) {
        log.info("mutator fired on 500");
        executeMutator(counterMutator, rows);
      }
    }
    executeMutator(counterMutator,0);
    commandRunner.doneSignal.countDown();

    return null;  
  }


  class CounterColumnBuilder {
    private HCounterColumn<String> clicksCounter;
    private HCounterColumn<String> viewsCounter;
    private final String keyString;

    CounterColumnBuilder(BucketType bucketType, long rowNumber) {
      this.keyString = bucketType.formatDate(System.currentTimeMillis() + (rowNumber * 60 * 1000));
      log.info("using keyString {}",keyString);
    }

    CounterColumnBuilder applyClicks(long clicks) {
      this.clicksCounter = new HCounterColumnImpl<String>("clicks",clicks,StringSerializer.get());
      return this;
    }

    CounterColumnBuilder applyView(long views) {
      this.viewsCounter = new HCounterColumnImpl<String>("views",views,StringSerializer.get());
      return this;
    }

    void addToMutation(Mutator<String> mutator) {
      mutator.addCounter(keyString,"CounterCf",viewsCounter);
      mutator.addCounter(keyString,"CounterCf",clicksCounter);
    }
  }

  /**
   * Enum using commons-lang FastDateFormat. Parses a long key to a bucket value
   * based on such
   */
  enum BucketType {
    ALL("__ALL__"),
    MONTH("yyyy_MM"),
    WEEK("yyyy_MM_w"),
    DAY("yyyy_MM_dd"),
    HOUR("yyyy_MM_dd_hh"),
    MINUTE("yyyy_MM_dd_hh_mm");

    FastDateFormat formatter;

    BucketType(String format) {
      if ( !StringUtils.equals(format,"__ALL__")) {
        this.formatter = FastDateFormat.getInstance(format, TimeZone.getTimeZone("GMT"));
      }

    }

    /**
     * Return the formatterd date based on the type. In the case of __ALL__
     * we just return the format string, ignoring the date
     * @param date
     * @return
     */
    public String formatDate(long date) {
      if (this == BucketType.ALL ) {
        return toString();
      }
      return formatter.format(date);
    }

    @Override
    public String toString() {
      if ( formatter == null ) {
        return "__ALL__";
      }
      return formatter.getPattern();
    }
  }
}

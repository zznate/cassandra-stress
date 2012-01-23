package com.riptano.cassandra.stress;

import java.util.TimeZone;

import me.prettyprint.cassandra.model.HCounterColumnImpl;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.BatchSizeHint;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apache.commons.lang.time.FastDateFormat;

/**
 * Testing a scenario where HH *might* cause counter replication to loop
 * on moderately sized clusters
 *
 * @author zznate
 */
public class BucketingCounterSpreadCommand extends StressCommand {

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

    Mutator<String> counterMutator =
      HFactory.createMutator(commandArgs.keyspace, StringSerializer.get(), new BatchSizeHint(500,2));
    int x=0;
    for(; x < MINS_IN_YEAR; x++) {
      // build all bucket - 1 row
      new CounterColumnBuilder(BucketType.ALL,x)
        .applyClicks(1)
        .applyView(1)
        .addToMutation(counterMutator);

      // 12 rows
      new CounterColumnBuilder(BucketType.MONTH,x)
        .applyClicks(1)
        .applyView(1)
        .addToMutation(counterMutator);

      // 52 rows
      new CounterColumnBuilder(BucketType.WEEK,x)
        .applyClicks(1)
        .applyView(1)
        .addToMutation(counterMutator);

      // 365 rows
      new CounterColumnBuilder(BucketType.DAY,x)
        .applyClicks(1)
        .applyView(1)
        .addToMutation(counterMutator);

      // 8760 rows
      new CounterColumnBuilder(BucketType.HOUR,x)
        .applyClicks(1)
        .applyView(1)
        .addToMutation(counterMutator);

      // 525600 rows
      new CounterColumnBuilder(BucketType.MINUTE,x)
        .applyClicks(1)
        .applyView(1)
        .addToMutation(counterMutator);

      // TODO test≈ auto-batching here
      if ( x % 500 == 0) {
        executeMutator(counterMutator, x);
      }
    }
    executeMutator(counterMutator,x);


    return null;  
  }


  static class CounterColumnBuilder {
    private HCounterColumn<String> clicksCounter;
    private HCounterColumn<String> viewsCounter;
    private final String keyString;

    CounterColumnBuilder(BucketType bucketType, long minInYear) {
      this.keyString = bucketType.formatDate(minInYear * 60 * 1000);
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
      mutator.addCounter(keyString,"CounterCf",clicksCounter);
      mutator.addCounter(keyString,"CounterCf",viewsCounter);
    }
  }

  /**
   * Enum using commons-lang FastDateFormat. Parses a long key to a bucket value
   * based on such
   */
  enum BucketType {
    ALL("__ALL__"),
    MONTH("YYYY_MM"),
    WEEK("YYYY_MM_w"),
    DAY("YYYY_MM_dd"),
    HOUR("YYYY_MM_dd_hh"),
    MINUTE("YYYY_MM_dd_hh_mm");

    final FastDateFormat formatter;

    BucketType(String format) {
      this.formatter = FastDateFormat.getInstance(format, TimeZone.getTimeZone("GMT"));
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
      return formatter.getPattern();
    }
  }
}

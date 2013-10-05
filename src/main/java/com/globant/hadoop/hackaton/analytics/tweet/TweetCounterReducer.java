package com.globant.hadoop.hackaton.analytics.tweet;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Accumulates the total amount of tweets related to a keyword.
 * 
 * @author Juan Gentile
 * 
 */
public class TweetCounterReducer extends
    Reducer<Text, LongWritable, Text, LongWritable> {

  /** A holder used to write the output. */
  private static final LongWritable MENTIONS = new LongWritable();

  /**
   * The reduce method sums for each keyword the total number of mentions and
   * puts the keyword and its total number in the context.
   * 
   * @param key The keyword
   * @param values A list of occurrences counts
   * @throws IOException If the output cannot be written to the context.
   * @throws InterruptedException If the output cannot be written to the context.
   */
  @Override
  public void reduce(Text key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
    long sum = 0L;

    for (LongWritable value : values) {
       sum += value.get();
    }

    MENTIONS.set(sum);
    context.write(key, MENTIONS);
  }
}

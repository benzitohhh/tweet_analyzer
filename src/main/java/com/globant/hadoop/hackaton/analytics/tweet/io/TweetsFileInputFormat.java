package com.globant.hadoop.hackaton.analytics.tweet.io;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.globant.hadoop.hackaton.analytics.tweet.domain.Tweet;

/**
 * This class creates a new TweetRecordReader to parse Tweets.
 * 
 * @author Juan Gentile
 * 
 */
public class TweetsFileInputFormat extends FileInputFormat<LongWritable, Tweet> {

  /**
   * This method creates a new TweetRecordReader.
   * 
   * @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(org.apache.hadoop.mapreduce.InputSplit,
   *      org.apache.hadoop.mapreduce.TaskAttemptContext)
   */
  @Override
  public RecordReader<LongWritable, Tweet> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new TweetRecordReader();
  }
}

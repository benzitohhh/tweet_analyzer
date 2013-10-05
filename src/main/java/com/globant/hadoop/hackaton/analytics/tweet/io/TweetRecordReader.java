package com.globant.hadoop.hackaton.analytics.tweet.io;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.log4j.Logger;
import org.json.JSONException;

import com.globant.hadoop.hackaton.analytics.tweet.domain.Tweet;

/**
 * Parses tweets from input files.
 * Each line in the files is expected to be a Tweet in JSON format
 * as retrieved from the Twitter Streaming API.
 * 
 * @author Juan Gentile
 */
public class TweetRecordReader extends RecordReader<LongWritable, Tweet> {

  private static final Logger logger =
    Logger.getLogger(TweetRecordReader.class);

  /** This reader reads by line from the input. */
  private LineRecordReader lineRecordReader;

  /** Current read tweet. */
  private Tweet tweet = new Tweet();

  private boolean hasTweet = false;

  /**
   * @see org.apache.hadoop.mapreduce.RecordReader#close()
   */
  @Override
  public void close() throws IOException {
    this.lineRecordReader.close();
  }

  /**
   * Returns the key for the current Tweet.
   * This implementation returns the file offset as the key.
   * 
   * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
   * @return  The current file offset or null if no record has been found.
   */
  @Override
  public LongWritable getCurrentKey() {
    return this.lineRecordReader.getCurrentKey();
  }

  /**
   * Returns the current tweet.
   * 
   * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
   * @return The curren tweet or null if none has been found.
   */
  @Override
  public Tweet getCurrentValue() {
    if (this.hasTweet) {
      return tweet;
    } else {
      return null;
    }
  }

  /**
   * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
   */
  @Override
  public float getProgress() {
    return this.lineRecordReader.getProgress();
  }

  /**
   * @see org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit,
   *      org.apache.hadoop.mapreduce.TaskAttemptContext)
   */
  @Override
  public void initialize(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException {
    this.lineRecordReader = new LineRecordReader();
    this.lineRecordReader.initialize(inputSplit, taskAttemptContext);
  }

  /**
   * Parses the Tweet and if there is valid data return true. If the tweet is
   * invalid reads the next record, if there are no more valid records then
   * false is returned.
   * 
   * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
   */
  @Override
  public boolean nextKeyValue() throws IOException {
    this.hasTweet = false;

    while (!this.hasTweet && this.lineRecordReader.nextKeyValue()) {
      Text currentValue = this.lineRecordReader.getCurrentValue();
      try {
        this.tweet.loadTweet(currentValue.toString());
        this.hasTweet = true;
      } catch (JSONException e) {
        logger.warn(String.format("Invalid JSON [%s]", currentValue), e);
      } catch (IllegalArgumentException e) {
        logger.warn("Not a Tweet", e);
      }
    }

    return this.hasTweet;
  }
}

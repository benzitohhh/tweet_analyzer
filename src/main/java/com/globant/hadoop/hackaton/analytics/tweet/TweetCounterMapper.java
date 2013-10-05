package com.globant.hadoop.hackaton.analytics.tweet;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.globant.hadoop.hackaton.analytics.tweet.domain.Tweet;
import com.globant.hadoop.hackaton.analytics.tweet.io.KeywordsFileProvider;

/**
 * Maps Keyword occurrences.
 * 
 * @author Juan Gentile
 * 
 */
public class TweetCounterMapper extends
    Mapper<LongWritable, Tweet, Text, LongWritable> {

  /** A holder used to write the output. */
  private static final LongWritable VALUE_ONE = new LongWritable(1);
  /** A holder used to write the output. */
  private static final Text KEYWORD = new Text();
  /** Holds keywords related to the tweets collected. */
  private KeywordsFileProvider keywordsFileProvider;

  /**
   * Instantiates the KeywordProvider
   * 
   * @param context which provides the job configuration.
   * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
   * @throws IOException when there is a problem reading keywords file.
   */
  @Override
  protected void setup(Context context) throws IOException {
    this.keywordsFileProvider =
      new KeywordsFileProvider(context.getConfiguration());
  }

  /**
   * Process the value, extracting the keywords from it and putting them in the
   * context.
   * 
   * @param key is offset of the file.
   * @param value is the record with the information of the tweet.
   * @param context onto which the output is written.
   * @throws IOException if the output cannot be written to the context
   * @throws InterruptedException if the output cannot be written to the context
   */
  @Override
  public void map(LongWritable offset, Tweet value, Context context)
      throws IOException, InterruptedException {
    List<String> keywords =
      keywordsFileProvider.getMatchingKeywords(value.getText());

    for (String keyword : keywords) {
       KEYWORD.set(keyword);
       context.write(KEYWORD, VALUE_ONE);
    }
  }
}

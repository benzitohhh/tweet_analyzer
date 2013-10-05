package com.benimmanuel.hadoop.hackaton.analytics.tweetusers;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.globant.hadoop.hackaton.analytics.tweet.domain.Tweet;
import com.globant.hadoop.hackaton.analytics.tweet.io.KeywordsFileProvider;

public class UniqueUserMapper extends
    Mapper<LongWritable, Tweet, Text, Text> {

    /** A holder used to write the output. */
    private static final Text VALUE_USER = new Text();
    /** A holder used to write the output. */
    private static final Text KEYWORD = new Text();
    /** Holds keywords related to the tweets collected. */
    private KeywordsFileProvider keywordsFileProvider;
    
    @Override
    protected void setup(Context context) throws IOException {
      this.keywordsFileProvider =
        new KeywordsFileProvider(context.getConfiguration());
    }
    
    @Override
    public void map(LongWritable offset, Tweet value, Context context)
        throws IOException, InterruptedException {
      List<String> keywords =
        keywordsFileProvider.getMatchingKeywords(value.getText());

      for (String keyword : keywords) {
         KEYWORD.set(keyword);
         VALUE_USER.set(value.getUser().getScreenName());
         context.write(KEYWORD, VALUE_USER);
      }
    }
    
}

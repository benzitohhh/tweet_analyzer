package com.globant.hadoop.hackaton.analytics.tweet.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Represents a Tweet. This class loads values after instantiation so that
 * the same instance can be reused.
 *
 * @author juan.palacios
 */
public class Tweet implements Writable {

  /** The JSON property containing the tweeted text. */
  private static final String TEXT = "text";
  /** The JSON property containing the tweet author. */
  private static final String USER = "user";

  /** The content of the tweet. */
  private final Text text = new Text();
  /** The author of this tweet. */
  private final TweetUser user = new TweetUser();

  /**
   * Loads new values for this Tweet.
   *
   * @param json The JSON object representing a tweet as a String.
   * @throws JSONException if the json cannot be properly parsed.
   **/
  public void loadTweet(final String json) throws JSONException {
    JSONObject tweet = new JSONObject(json);
    Validate.isTrue(tweet.has(TEXT) && !tweet.isNull(TEXT),
      "Not a tweet. Text is Missing");
    Validate.isTrue(tweet.has(USER) && !tweet.isNull(USER),
        "Not a tweet. User is Missing");

    this.text.set(tweet.getString(TEXT));
    this.user.loadUser(tweet.getJSONObject(USER));
  }

  /** @return the tweeted text. */
  public String getText() {
    return this.text.toString();
  }
  
  public TweetUser getUser() {
    return this.user;
  }   

  /**
   * Used by Hadoop to serialize this object.
   * @see Writable#write(DataOutput)
   */
  @Override
  public void write(final DataOutput out) throws IOException {
    this.text.write(out);
    this.user.write(out);
  }

  /** @return the friends count for the author of this tweet. */
  public Integer getAuthorFriendsCount() {
    return this.user.getFriendsCount();
  }

  /**
   * Used by Hadoop to deserialize this object.
   * @see Writable#readFields(DataInput)
   */
  @Override
  public void readFields(final DataInput in) throws IOException {
    this.text.readFields(in);
    this.user.readFields(in);
  }
}

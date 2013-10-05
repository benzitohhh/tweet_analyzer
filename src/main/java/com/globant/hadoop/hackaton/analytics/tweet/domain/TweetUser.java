package com.globant.hadoop.hackaton.analytics.tweet.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.json.JSONException;
import org.json.JSONObject;

/** Represents the author of a Tweet. */
public class TweetUser implements Writable {

  /** The JSON property containing the friends count for this user. */
  private static final String FRIENDS_COUNT = "friends_count";

  /** The JSON property containing the screen name for this user. */
  private static final String SCREEN_NAME = "screen_name";
  
  /**
   * An invalid friends count mark that there was no information
   * about the user's followers.
   **/
  private static final int INVALID_FRIENDS_COUNT = -1;

  /** This user's friends count. */
  private IntWritable friendsCount = new IntWritable();
  
  private Text screenName = new Text();

  /**
   * Loads the user metadata.
   * @param json The JSON object containing the user metadata.
   **/
  public void loadUser(final JSONObject json) throws JSONException {
    int friendsCount = json.isNull(FRIENDS_COUNT) ?
        INVALID_FRIENDS_COUNT : json.getInt(FRIENDS_COUNT);
    this.friendsCount.set(friendsCount);
    String screenName = json.getString(SCREEN_NAME);
    this.screenName.set(screenName);
  }

  /** @return the friends count. */
  public Integer getFriendsCount() {
    int friendsCount = this.friendsCount.get();
    return friendsCount == INVALID_FRIENDS_COUNT ? null : friendsCount;
  }

  /** @return the users screen name */
  public String getScreenName() {
    return this.screenName.toString();
  }
  
  /**
   * Used by Hadoop to serialize this object.
   * @see Writable#write(DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException {
      this.screenName.write(out);
      this.friendsCount.write(out);
  }

  /**
   * Used by Hadoop to deserialize this object.
   * @see Writable#readFields(DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException {
      this.friendsCount.readFields(in);
      this.screenName.readFields(in);
  }

}

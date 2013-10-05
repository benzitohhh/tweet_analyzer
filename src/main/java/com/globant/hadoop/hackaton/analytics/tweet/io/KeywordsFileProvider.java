package com.globant.hadoop.hackaton.analytics.tweet.io;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.security.ProviderException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

/**
 * The Keywords provider should provide the keywords that will be sought within
 * the comments.
 * This provider reads the keywords from a file, stored in Hadoop's distributed
 * cache.
 * 
 * @author emiliano
 */
public class KeywordsFileProvider {

  /**
   * The list where keywords are stored.
   * All keywords must be stored in lower case to reduce memory usage.
   */
  private List<String> keywords = new ArrayList<String>();
  /**
   * A list where matching keywords will be stored.
   * This is an instance variable to avoid creating a list every time we
   * search for matches. 
   */
  private List<String> matchingKeywords = new ArrayList<String>();

  /**
   * Creates a provider based on the given configuration
   * 
   * @param conf
   *          The hadoop Configuration, from where the cached files will be read
   * @throws IOException
   *           Throws the exception when it's not able to retrieve the keywords
   *           file.
   * @throws ProviderException
   *           Throws the exception when the config file contains one or more
   *           lines without an '='.
   */
  public KeywordsFileProvider(Configuration conf) throws IOException {

    Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);

    if (null != cacheFiles) {

      loadKeywords(cacheFiles[0]);

    }
  }

  /**
   * Loads the keywords into the keyword map.
   * 
   * @param cachePath
   *          Path from which the keywords will be loaded.
   * @throws IOException
   *           Throws the exception when it's not able to retrieve the keywords
   *           file.
   */
  private void loadKeywords(Path cachePath) throws IOException,
      ProviderException {

    BufferedReader wordReader = new BufferedReader(new FileReader(
        cachePath.toString()));

    try {
      String line;

      while ((line = wordReader.readLine()) != null) {
        keywords.add(line.toLowerCase());
      }
    } finally {
      wordReader.close();
    }

  }

  /**
   * This method searches the keywords in the text and return the list of all
   * the keywords found.
   * 
   * @param text
   *          . Contains the text where to search for the keywords. Cannot be
   *          null.
   * @return the list of keywords that appear in the text received.
   */
  public List<String> getMatchingKeywords(String text) {
    Validate.notNull(text, "Text cannot be null");

    matchingKeywords.clear();

    String lowerText = text.toLowerCase();
    for (String keyword : keywords) {
      if (lowerText.contains(keyword)) {
        matchingKeywords .add(keyword);
      }
    }
    return matchingKeywords ;
  }
}

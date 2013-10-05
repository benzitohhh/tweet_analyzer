package com.globant.hadoop.hackaton.analytics.tweet;

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.globant.hadoop.hackaton.analytics.tweet.io.TweetsFileInputFormat;

/**
 * This class launches the mapReduce job.
 * 
 * @author Juan Gentile
 * 
 */
public class TweetCounterMapReduce {

    private static final String TWEET_COUNTER_KEYWORDS_PATH              = "tweet.counter.keywords.path";

    private static final String TWEET_COUNTER_PROPERTIES                 = "tweet-analyzer.properties";

    /** This character is used as separator in final results */
    static final String         KEY_VALUE_SEPARATOR                      = ",";

    /** Property to set the key value char separator. */
    static final String         OUTPUT_KEY_VALUE_SEPARATOR_PROPERTY_NAME = "mapred.textoutputformat.separator";

    /** Job Name to be set in the job description */
    private static final String JOB_NAME                                 = "Keyword Count";

    /** Job to set the configuration of the mapReduce. */
    private Job                 job;

    /**
     * Default constructor that sets the job.
     * 
     * @param job
     *            This MapReduce job configuration.
     * @throws IllegalArgumentException
     *             if job is null.
     */
    public TweetCounterMapReduce(final Job job) {
        Validate.notNull(job, "Job can't be null");
        this.job = job;
    }

    /**
     * Main method to be called by hadoop to start the mapReduce.
     * 
     * @param args
     *            The first element in args is the input path, the second is the
     *            output path.
     * @throws Exception
     *             if there is problem running the job.
     */
    public static void main(String[] args) throws Exception {
        int result = runMapReduce(args);
        System.exit(result);
    }

    /**
     * This method runs Tweet Counter Map Reduce job.
     * 
     * @param args
     *            The first element in args is the input path, the second is the
     *            output path.
     * @return 0 if it ran ok. Different from 0 otherwise.
     * @throws Exception
     *             if there is problem running the job.
     */
    public static int runMapReduce(String[] args) throws Exception {
        Configuration conf = new Configuration();
        TweetCounterMapReduce tweetCounterMapReduce = new TweetCounterMapReduce(
                new Job(conf, JOB_NAME));
        return tweetCounterMapReduce.run(args);
    }

    /**
     * This method sets the job configuration; mapper and reducer classes, input
     * and output paths, input and output value types and starts the main job.
     * 
     * @param args
     *            First element in args is the input path and the second is the
     *            output path.
     * @throws IOException
     *             if there is a problem opening the source file.
     * @throws ClassNotFoundException
     *             if there is a problem running the job.
     * @throws InterruptedException
     *             if there is a problem running the job.
     * @throws IllegalArgumentException
     *             if args is invalid.
     * @throws URISyntaxException
     *             if the keywords file's URI is invalid
     */
    public int run(String[] args) throws IOException, ClassNotFoundException,
            InterruptedException, URISyntaxException {
        Validate.notNull(args, "Parameters cannot be null.");
        Validate.isTrue((args.length == 2),
                "There should be 2 arguments: <INPUT_FOLDER> <OUTPUT_FOLDER>.");
        initialize(args);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Sets the initial configuration of the map reduce job.
     * 
     * @param args
     *            The configuration arguments.
     * @throws IOException
     *             if there is a problem adding the input path.
     */
    private void initialize(String[] args) throws IOException {

        Properties properties = new Properties();
        properties.load(new FileReader(TWEET_COUNTER_PROPERTIES));
        String keywordsFilePath = properties
                .getProperty(TWEET_COUNTER_KEYWORDS_PATH);
        DistributedCache
                .setLocalFiles(job.getConfiguration(), keywordsFilePath);

        job.getConfiguration().set(OUTPUT_KEY_VALUE_SEPARATOR_PROPERTY_NAME,
                KEY_VALUE_SEPARATOR);

        job.setJarByClass(TweetCounterMapReduce.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(TweetsFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class); // standard Hadoop class
        
        job.setMapperClass(TweetCounterMapper.class);
        job.setCombinerClass(TweetCounterReducer.class);
        job.setReducerClass(TweetCounterReducer.class);

        // NOTE: must explicitly set output key classes,
        // otherwise defaults are used
        // (which are likely different to your map/reduce)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

    }
}

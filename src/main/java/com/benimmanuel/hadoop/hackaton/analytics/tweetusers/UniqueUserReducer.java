package com.benimmanuel.hadoop.hackaton.analytics.tweetusers;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UniqueUserReducer extends Reducer<Text, Text, Text, ArrayWritable> {

    /** A holder used to write the output. */
    private static final ArrayWritable SCREEN_NAMES = new MyArrayWritable(
                                                            Text.class);

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
        // get unique names
        Set<Text> names = new HashSet<Text>();
        for (Text value : values) {
            names.add(new Text(value));
        }

        // output (as ArrayWritable of Text objects)
        Text[] aNames = names.toArray(new Text[names.size()]);
        SCREEN_NAMES.set(aNames);
        context.write(key, SCREEN_NAMES);
    }
}

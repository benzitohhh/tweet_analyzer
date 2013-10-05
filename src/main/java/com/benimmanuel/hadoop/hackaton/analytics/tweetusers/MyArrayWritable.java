package com.benimmanuel.hadoop.hackaton.analytics.tweetusers;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class MyArrayWritable extends ArrayWritable {

    public MyArrayWritable(Class<? extends Writable> valueClass) {
        super(valueClass);
    }
    
    @Override
    public String toString() {
        return StringUtils.join(toStrings(), ",");
    }
}

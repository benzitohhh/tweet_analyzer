package com.benimmanuel;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.io.Text;

public class Experiment {
    
    public static class RandomString {
        private final static char[] symbols = new char[36];
        
        static {
            for (int idx = 0; idx < 10; ++idx) {
                symbols[idx] = (char) ('0' + idx);
            }
            for (int idx = 10; idx < 36; ++idx) {
                symbols[idx] = (char) ('a' + idx -10);
            }
        }
        
        private final Random random = new Random();
        
        private final char[] buf;
        
        public RandomString(int length) {
            buf = new char[length];
        }
        
        public String nextString() {
            for (int i = 0; i < buf.length; i++) {
                buf[i] = symbols[random.nextInt(symbols.length)];
            }
            return new String(buf);
        }
    }
    
    public static void testMemory() {
        final int LENGTH = 20; // for LENGTH=20 (20 char name strings)
        final int N = 1000000; // with 2048M heap space
        // my PC maxes out at N=8mm
        // - approx 100megs for every 1mm names
        final RandomString random = new RandomString(LENGTH);
        
        // get lots of short strings
        String[] names = new String[N];
        for (int i = 0; i < names.length; i++) {
            names[i] = random.nextString();
        }
        
        // make sure there are lots of duplicates
        int m = names.length / 2;
        for (int i = 0; i < m; i++) {
            names[i+m] = names[i];
        }
        
        // get uniques
        Set<String> uNames = new HashSet<String>(Arrays.asList(names));
        
        System.out.println("uNames.size = " + uNames.size());
    }
    
    public static void testTextEquality() {
        Text a = new Text("blap");
        Text b = new Text("brap");
        Text c = new Text("brap");
        
        boolean eq  = a.equals(b); // false - so far, so good
        boolean eq2 = b.equals(c); // true  - so far, so good
        
        // try adding in a Set
        Set<Text> set = new HashSet<Text>();
        //set.add(a);
        //set.add(b);
        set.add(c);
        
        System.out.println("yay");
        // set is {brap, blap}  // looks good to me.
    }
    
    public static void main(String[] args) {
        //testMemory();
        testTextEquality();
    }
}

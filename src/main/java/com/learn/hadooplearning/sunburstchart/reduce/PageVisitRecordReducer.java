package com.learn.hadooplearning.sunburstchart.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageVisitRecordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int totalOccurences = 0;
        for (IntWritable counter : values){
            totalOccurences += counter.get();
        }
        context.write(key, new IntWritable(totalOccurences));
    }
}
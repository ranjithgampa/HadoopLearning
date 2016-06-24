package com.learn.hadooplearning.mapsidejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortByKeyReducer extends Reducer<Text,Text,NullWritable,Text> {

    private static final NullWritable nullKey = NullWritable.get();

    public void reduce(Text key, Iterable<Text> values, Mapper.Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(nullKey,value);
        }
    }
}
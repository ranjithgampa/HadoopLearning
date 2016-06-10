package com.cardinalhealth.hadooplearning.movieratings.reducesidejoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MovieMapper extends Mapper<LongWritable,Text, IntWritable, Text> {
    public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
        String[] fields = line.toString().split("\\|");
        context.write(new IntWritable(Integer.parseInt(fields[0])),new Text(fields[1]+"-"+fields[2]));
    }
}

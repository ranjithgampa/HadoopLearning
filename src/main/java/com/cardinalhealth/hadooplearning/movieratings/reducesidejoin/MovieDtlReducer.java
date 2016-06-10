package com.cardinalhealth.hadooplearning.movieratings.reducesidejoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MovieDtlReducer extends Reducer<IntWritable, Text, Text, Text>{
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int length = 0;
        String value ="";
        while(values.iterator().hasNext()){
            length = length + 1;
            value = values.iterator().next().toString();
        }
        if(length == 1){
            String[] fields = value.split("-");
            context.write(new Text(fields[0]), new Text(fields[1]));
        }
    }
}

package com.cardinalhealth.hadooplearning.runner;

import com.cardinalhealth.hadooplearning.InputFormat.PageVisitInputFormat;
import com.cardinalhealth.hadooplearning.map.PageVisitRecordMap;
import com.cardinalhealth.hadooplearning.reduce.PageVisitRecordReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

public class PageVisitRecordRunner {

	public static Logger logger = Logger.getLogger("com.cardinalhealth.hadooplearning.appLogger");
    public static void main(String[] args) throws Exception {
    	logger.info("logger working");
        Configuration conf = new Configuration();

        Job job = new Job(conf);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(PageVisitRecordMap.class);
        job.setReducerClass(PageVisitRecordReducer.class);

        job.setInputFormatClass(PageVisitInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setJarByClass(PageVisitRecordRunner.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success= job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }

}
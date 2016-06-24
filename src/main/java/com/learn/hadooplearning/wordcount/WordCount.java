package com.learn.hadooplearning.wordcount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;

public class WordCount extends Configured implements Tool {
    public static Logger logger = Logger.getLogger("com.learn.hadooplearning.appLogger");

    public int run(String[] args) throws Exception {
        deletePrevOutput(args[1]);
        Job job = Job.getInstance(getConf());
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setJarByClass(WordCount.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public void deletePrevOutput(String output) throws IOException {
        FileSystem fs = FileSystem.get(getConf());
        fs.delete(new Path(output), true);
    }

    public static void main(String[] args) throws Exception {
        WordCount driver = new WordCount();
        int exitCode = ToolRunner.run(driver, args);
        System.exit(exitCode);
    }
}

package com.cardinalhealth.hadooplearning.movieratings.reducesidejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class MovieDtlDriver extends Configured implements Tool {

    public void deletePrevOutput(String output) throws IOException {
        System.out.println("Deleting prev output");
        FileSystem fs= FileSystem.get(getConf());
        fs.delete(new Path(output), true);
    }

    public static void main(String[] args) throws Exception {
        MovieDtlDriver driver = new MovieDtlDriver();
        int exitCode = ToolRunner.run(driver, args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        deletePrevOutput(args[2]);
        Configuration conf = getConf();

        Job job = Job.getInstance(conf);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setJarByClass(MovieDtlDriver.class);
        job.setReducerClass(MovieDtlReducer.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieRatingsMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;

    }
}

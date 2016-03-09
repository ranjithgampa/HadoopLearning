package com.cardinalhealth.hadooplearning.InputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.cardinalhealth.hadooplearning.recordReader.PageVisitRecrodReader;

public class PageVisitInputFormat extends FileInputFormat<LongWritable,Text> {
    @Override
    public RecordReader<LongWritable,Text> createRecordReader(InputSplit split, TaskAttemptContext context){
        return new PageVisitRecrodReader();
    }
}

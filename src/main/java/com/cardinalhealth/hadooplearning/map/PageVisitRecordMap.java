package com.cardinalhealth.hadooplearning.map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.cardinalhealth.hadooplearning.util.Record;

import java.io.IOException;

public class PageVisitRecordMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	public static Logger logger = Logger.getLogger("com.cardinalhealth.hadooplearning.appLogger");
	
	@Override
	public void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String content = value.toString();
		String[] lines = content.split("\n");
		String output = "";
		for (String line : lines) {
			if(isInvalidLine(line)){
				dealWithInvalidLine(context, line);
				continue;
			}
			Record record = new Record(line);
			String screenName = record.getUrl().getScreenIdentity();
			
			if (StringUtils.isEmpty(screenName)) continue;
			
			if(StringUtils.isEmpty(output)){
				output = screenName;
			}else{
				output = output + "-" + screenName;
			}
		}
		context.write(new Text(condenseOutput(output)), new IntWritable(1));
		context.write(new Text(" No of lines to Map are"),new IntWritable(lines.length));
	}
	private String condenseOutput(String output){
		return output.replaceAll("(.+)\\1+", "$1").replaceAll("(.+)\\1+", "$1").replaceAll("(.+)\\1+", "$1").replaceAll("(.+)\\1+", "$1").replaceAll("(.+?)\\1+", "$1");
	}
	
	private void dealWithInvalidLine( Context context, String line) throws IOException, InterruptedException{
		context.write(new Text(line), new IntWritable(1));
	}
	
	private boolean isInvalidLine(String line){
		return line.startsWith("Invalid record sent");
	}
}

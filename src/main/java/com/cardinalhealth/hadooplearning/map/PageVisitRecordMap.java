package com.cardinalhealth.hadooplearning.map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class PageVisitRecordMap extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	public static Logger logger = Logger.getLogger("com.cardinalhealth.hadooplearning.appLogger");
	
	@Override
	public void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String content = value.toString();
		String output = null;
		for (String line : content.split("\n")) {
			//System.out.println(line);
			String screenName = getScreenName(line);
			if(StringUtils.isEmpty(screenName)){
				System.out.println(" Culprit records are");
				System.out.println(content);
				return;
			}
			if (output == null) {
				output = screenName;
			} else if (!StringUtils.isEmpty(screenName)) {
				output = output + "-" + getScreenName(line);
			}
		}
		context.write(new Text(output), new IntWritable(1));
		context.write(new Text(" No of lines to Map are"),new IntWritable(content.split("\n").length));
	}

	private String getScreenName(String line) {
		String[] tokens = line.split(",");
		if (tokens.length >= 5) {
			String url = (line.split(","))[4];
			return massageUrl(url);
		} else {
			return "";
		}
	}
	
	public String removeQueryParameter(String url){
		if(url.indexOf("?")!=-1){
			return url.substring(0, url.indexOf("?")) ;
		}else{
			return url;
		}
	}
	public String mssageVarianceUrl(String url){
		if(url.contains("variance/")){
			return url.substring(0, url.lastIndexOf('/'));
		}
		return url;
	}
	
	public String getURLPortionAfterModuleName(String url){
		if (url.indexOf("#!") != -1) {
			return url.substring(url.indexOf("/",url.indexOf("#!"))+1);
		} else {
			return "";
		}
	}
	
	public String massageUrl(String url){
		url = removeQueryParameter(url);
		url = getURLPortionAfterModuleName(url);
		url = mssageVarianceUrl(url);
		return url;
	}
}

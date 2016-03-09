package com.cardinalhealth.hadooplearning.recordReader;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.Logger;

public class PageVisitRecrodReader extends RecordReader<LongWritable, Text>{
	public static Logger logger = Logger.getLogger("com.cardinalhealth.hadooplearning.appLogger");
    private Integer maxLengthRecord;
    private LongWritable key = new LongWritable();
    Text value = new Text();
    private final static Text EOL = new Text("\n");
    private long start =0, end =0 , pos =0;
    private LineReader in;
    FSDataInputStream fileIn;
    private Configuration config;
    private Text prevRecord;
    private Text firstRecordOfNextBatch;
    
    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        config = context.getConfiguration();
        FileSplit split = (FileSplit) genericSplit;
        Path file = split.getPath();
        FileSystem fileSystem = file.getFileSystem(config);
        fileIn = fileSystem.open(file);

        this.maxLengthRecord = config.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        in = new LineReader(fileIn,config);
        this.pos = start;
        System.out.println("Split limits Start: " + start +" end: " + end);
        if (start != 0){
        	fileIn.seek(start);
        }
/*  
        boolean skipFirstLine = false;
        if (start != 0){
            skipFirstLine = true;
            --start;
            fileIn.seek(start);
        }
      if(skipFirstLine){
      start += in.readLine(new Text(),0,(int)Math.min((long)Integer.MAX_VALUE, end - start));
  }
*/        
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(isEndOfBlock()){
        	return false;
        }
        initializeKeyValue();
        appendDelimiterRecord();
        Text lineContent = null;
        while(true){
        	if(isEndOfBlock()){
            	break;
            }
        	if(lineContent!=null){
        		prevRecord = lineContent;
        	}
            lineContent = readALine();
            if( isItEndOfTheRecordGroup(lineContent)){
            	firstRecordOfNextBatch = lineContent;
                break;
            }
            appendLineToRecordSet(lineContent);
         }
        prevRecord = null;
        return true;
    }
    
    private boolean isEndOfBlock(){
    	return pos >= end;
    }
	private boolean isItEndOfTheRecordGroup(Text lineContent) {
		return isNotTheVeryFirstRecord() && isUserBackToQueue(lineContent.toString()) || isEndOfCurrentSession(lineContent.toString());
	}

	private boolean isNotTheVeryFirstRecord() {
		return value.getLength()!=0;
	}
    public Text readALine() throws IOException {
        Text line = new Text();
        int newSize = in.readLine(line, maxLengthRecord,Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),maxLengthRecord));
        pos += newSize;
        return line;
    }

    private void appendLineToRecordSet(Text lineContent) {
        value.append(lineContent.getBytes(),0, lineContent.getLength());
        value.append(EOL.getBytes(),0, EOL.getLength());
    }
    private boolean isEndOfCurrentSession(String row){
    	return prevRecord!=null && !getUserSessionId(row).equalsIgnoreCase(getUserSessionId(prevRecord.toString()));
    }
    private boolean isUserBackToQueue(String row){
    	return getUrlFromTheRow(row).endsWith("queue");
    }
    private String getUserSessionId(String row){
        String[] fields = row.split(",");
        if(fields.length>=1){
            return fields[0];
        }
        return null;
    }
    private String getUrlFromTheRow(String row){
        String[] fields = row.split(",");
        if(fields.length>=5){
            return fields[4];
        }
        return null;
    }
    private void initializeKeyValue() {
        if(key == null) { key = new LongWritable();}
        if(value == null){value = new Text();}

        value.clear();
        key.set(pos);
    }

	private void appendDelimiterRecord() {
		if(firstRecordOfNextBatch!=null){
        	appendLineToRecordSet(firstRecordOfNextBatch);
        	firstRecordOfNextBatch = null;
        }
	}
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if(start == end){
            return 0.0f;
        }else{
           return Math.min(1.0f, pos-start/end-start);
        }
    }

    @Override
    public void close() throws IOException {
        if(in!=null){
            in.close();
        }
    }
}

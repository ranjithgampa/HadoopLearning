package com.cardinalhealth.hadooplearning.sunburstchart.recordReader;

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

import com.cardinalhealth.hadooplearning.sunburstchart.util.Record;

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
        	if(!isRecordValid(lineContent)){
        		lineContent = new Text("Invalid record sent " + lineContent.toString());
        		appendLineToRecordSet(lineContent);
        		lineContent=null;
        		continue;
        	}
          
            if( isItEndOfTheRecordGroup(lineContent)){
            	firstRecordOfNextBatch = lineContent;
                break;
            }
            appendLineToRecordSet(lineContent);
         }
        prevRecord = null;
        return true;
    }
    
    private boolean isRecordValid(Text lineContent) {
    	try{
    		new Record(lineContent.toString());
    	}catch(Exception e){
    		return false;
    	}
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
        int newSize = in.readLine(line, maxLengthRecord,  Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
        		 													maxLengthRecord
        		 												  )
        		 				 );
        pos += newSize;
        return line;
    }

    private void appendLineToRecordSet(Text lineContent) {
        value.append(lineContent.getBytes(),0, lineContent.getLength());
        value.append(EOL.getBytes(),0, EOL.getLength());
    }
    private boolean isEndOfCurrentSession(String row){
    	if(prevRecord == null ) return false;
    	Record record = null , prevRec = null;
		record = new Record(row);
		prevRec = new Record(prevRecord.toString());
    	return !record.getUserSessionId().equalsIgnoreCase(prevRec.getUserSessionId());
    }
    private boolean isUserBackToQueue(String row){
    	Record record = new Record(row);
    	return record.getUrl().getCompleteUrl().endsWith("queue") || record.getUrl().getCompleteUrl().endsWith("RxeView/");
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

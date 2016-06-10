package com.cardinalhealth.hadooplearning.sunburstchart.util;

public class Record {
	private String[] recordSplit;
	private URL url;
	private int noOfExpectedFieldsInRecord = 8;

	public Record(String recordString){
		this.recordSplit = recordString.replace("\\,", " ").split(",");
		if(recordSplit.length != noOfExpectedFieldsInRecord) {
			System.out.println("problematic record is " + recordString);
			throw new RuntimeException("Invalid record");
		}
		setUrl(new URL(recordSplit[4]));
	}

	public String getUserSessionId(){
		return recordSplit[0];
	}

	public URL getUrl() {
		return url;
	}

	public void setUrl(URL url) {
		this.url = url;
	}

}

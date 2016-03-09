package com.cardinalhealth.hadooplearning.util;

public class Record {
	private String[] recordSplit;
	private String url;
	private int noOfExpectedFieldsInRecord = 8;

	public Record(String recordString){
		this.recordSplit = recordString.split(",");
		if(recordSplit.length != noOfExpectedFieldsInRecord) {
			throw new RuntimeException("Invalid record");
		}
	}
	
	private String getUrl(){
		if(url==null){
			url =recordSplit[4];
		}
        return url;
	}
	
	public String getUserSessionId(){
		return recordSplit[0];
	}
	
	private String getScreenName(String line) {
		url = removeQueryParameter(url);
		url = getURLPortionAfterModuleName(url);
		url = mssageVarianceUrl(url);
		return url;
	}
	
	private String removeQueryParameter(String url){
		if(url.indexOf("?")!=-1){
			return url.substring(0, url.indexOf("?")) ;
		}else{
			return url;
		}
	}
	private String mssageVarianceUrl(String url){
		if(url.contains("variance/")){
			return url.substring(0, url.lastIndexOf('/'));
		}
		return url;
	}
	
	private String getURLPortionAfterModuleName(String url){
		if (url.indexOf("#!") != -1) {
			return url.substring(url.indexOf("/",url.indexOf("#!"))+1);
		} else {
			return "";
		}
	}		
}

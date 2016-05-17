package com.cardinalhealth.hadooplearning.util;

import org.apache.commons.lang.StringUtils;

public class URL {
	private String serverName;
	private String moduleName;
	private String subModuleName;
	private String screenName;
	private String actionName;
	private String completeUrl;

	public URL(String url) {
		this.completeUrl = url;
		parseUrl();
	}
	
	public void parseUrl(){
		String temp = removeQueryParameter(completeUrl);
		String[]  split = temp.split("/");
		this.serverName = split[0];
		this.moduleName = split[1];
		if(split.length > 2){
			this.subModuleName = split[2].replace("#!", "");
		}
		
		if(split.length > 3){
			this.screenName = split[3];
		}
		if(split.length > 4 && !StringUtils.isNumeric(split[4])){
			this.actionName = split[4];
		}else if(split.length > 5 && !StringUtils.isEmpty(split[5]) && !StringUtils.isNumeric(split[5])){
			this.actionName = split[5];
		}
	}
	private String removeQueryParameter(String url){
		if(url.indexOf("?")!=-1){
			return url.substring(0, url.indexOf("?")) ;
		}else{
			return url;
		}
	}
	public String getServerName() {
		return serverName;
	}

	public void setServerName(String serverName) {
		this.serverName = serverName;
	}

	public String getModuleName() {
		return moduleName;
	}

	public void setModuleName(String moduleName) {
		this.moduleName = moduleName;
	}

	public String getSubModuleName() {
		return subModuleName;
	}

	public void setSubModuleName(String subModuleName) {
		this.subModuleName = subModuleName;
	}

	public String getScreenName() {
		return screenName;
	}

	public void setScreenName(String screenName) {
		this.screenName = screenName;
	}

	public String getActionName() {
		return actionName;
	}

	public void setActionName(String actionName) {
		this.actionName = actionName;
	}

	public String getCompleteUrl() {
		return completeUrl;
	}

	public void setCompleteUrl(String completeUrl) {
		this.completeUrl = completeUrl;
	}
	
	public String toString(){
		return completeUrl;
	}
	
	public String getScreenIdentity(){
		String output ="";
		output += this.moduleName;
		if(subModuleName!=null) output += "/"+ this.subModuleName;
		if(screenName!=null) output += "/"+ this.screenName;		
		if(actionName!=null) output += "/"+ this.actionName;
		
		if(output.equalsIgnoreCase("RxeView")) screenName = "RxeView/ordering/queue";

		return  output;
	}
}
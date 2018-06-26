package com.liuyang.xdr.protocol.client;

import java.io.IOException;
import java.util.Properties;

import com.liuyang.xdr.protocol.Reader;

public class Response {
	private Properties header;
    private BaseClient client;
    
    public Response(BaseClient client) {
    	this.client = client;
    	this.header = new Properties();
    }
    
    protected void finalize() {
    	header = null;
    	client = null;
    }
	
	public boolean containsKey(Object key) {
		return header.containsKey(key);
	}
	
	public Reader getReader() throws IOException {
		return client.getReader();
	}
	
	public String getProperty(String key) {
		return header.getProperty(key);
	}
	
	public String getMethod() {
		return header.getProperty("METHOD");
	}
	
	public void setAttribute(String key, Object value) {
		header.put(key, value);
	}
	
	public Object getAttribute(String key) {
		return header.get(key);
	}
	
	public String getString(String key) {
		return String.valueOf(header.get(key));
	}
	
	public String toString() {
		return String.valueOf(header);
	}
}

package com.liuyang.xdr.protocol.server;

import java.io.IOException;
import java.util.Properties;

import com.liuyang.xdr.protocol.Reader;

public class Request {
    private Properties header;
    private Session session;
    
    public Request(Session session) {
    	this.session = session;
    	this.header = new Properties();
    }
    
    protected void finalize() {
    	header = null;
    	session = null;
    }
	
	public synchronized final boolean containsKey(Object key) {
		return header.containsKey(key);
	}
	
	public synchronized final Reader getReader() throws IOException {
		return session.getReader();
	}
	
	public synchronized final String getProperty(String key) {
		return header.getProperty(key);
	}
	
	public synchronized final String getMethod() {
		return header.getProperty("METHOD");
	}
	
	public synchronized final void setAttribute(String key, Object value) {
		header.put(key, value);
	}
	
	public synchronized final Object getAttribute(String key) {
		return header.get(key);
	}
	
	public synchronized final String getString(String key) {
		return String.valueOf(header.get(key));
	}
	
	public synchronized final String toString() {
		return String.valueOf(header);
	}
}

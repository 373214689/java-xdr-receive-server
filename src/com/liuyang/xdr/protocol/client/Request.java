package com.liuyang.xdr.protocol.client;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import com.liuyang.xdr.protocol.Writer;

public class Request {
	private Properties header;
	private BaseClient client;
	
	public Request(BaseClient client) {
		this.client = client;
		this.header = new Properties();
		
	}
	
	protected void finalize() {
		header = null;
		client = null;
	}
	
	public Writer getWriter() throws IOException {
		return client.getWriter();
	}
	
	public void setHeader(String key, Object value) {
		header.put(key, value);
	}
	
	public void setMethod(String value) {
		header.put("METHOD", value);
	}
	
	public void setStatus(String value) {
		header.put("STATUS", value);
	}
	
	public Object get(Object key) {
		return header.get(key);
	}
	
	public Set<Object> keys() {
		return header.keySet();
	}
	
	public Properties getHeader() {
		return header;
	}
}

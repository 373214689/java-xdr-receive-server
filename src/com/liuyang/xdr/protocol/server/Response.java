package com.liuyang.xdr.protocol.server;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import com.liuyang.xdr.protocol.Writer;

public class Response {
	private Properties header;
	private Session session;
	
	public Response(Session session) {
		this.session = session;
		this.header = new Properties();
		
	}
	
	protected void finalize() {
		header = null;
		session = null;
	}
	
	public synchronized Writer getWriter() throws IOException {
		return session.getWriter();
	}
	
	public synchronized void setHeader(String key, Object value) {
		header.put(key, value);
	}
	
	public synchronized  void setMethod(String value) {
		header.put("METHOD", value);
	}
	
	public synchronized  void setStatus(String value) {
		header.put("STATUS", value);
	}
	
	public synchronized Object get(Object key) {
		return header.get(key);
	}
	
	public synchronized  Set<Object> keys() {
		return header.keySet();
	}
	
	public synchronized Properties getHeader() {
		return header;
	}

	public synchronized Session getSession() {
		return session;
	}
	
	public String toString() {
		return String.valueOf(header);
	}
}

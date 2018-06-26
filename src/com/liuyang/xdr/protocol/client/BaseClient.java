package com.liuyang.xdr.protocol.client;

import java.net.Socket;
import java.util.Properties;
import java.util.function.Function;

import com.liuyang.xdr.protocol.Channel;


public class BaseClient extends Channel {
	private Properties conf;
	private String cookie;
	private BaseClient self = this;
	
	private Function<? super BaseClient, Response> responsePaser;
	
	public BaseClient(Socket client, Function<? super BaseClient, Response> action) {
		super(client, false);
		responsePaser = action;
		conf  = new Properties();
		// 如果指定了request的解析函数，则开始监听request
		/*if (responsePaser != null) {
			listen(channel -> {
				Response response = response();
	            if (response == null) return;
				if (!response.containsKey("METHOD")) return;
				if (!response.containsKey("COOKIE")) return;
				if (!cookie.equals(response.getProperty("COOKIE"))) return;
				handle(response);
			});
		}*/
	}
	
	public Response response() {
		if (responsePaser != null) {
			return responsePaser.apply(self);
		}
		return null;
	}
	
	public void send(Request request) {
		for(Object key : request.keys()) {
			writeUTF(key + ": " + request.get(key) + "\r\n");
		}
		writeUTF("\r\n");
	}
	
	public String cookie() {
		return cookie;
	}
	
	public void setAttribute(Object key, Object value) {
		conf.put(key, value);
	}
	
	public Object getAttribute(Object key) {
		return conf.get(key);
	}
	
	
}

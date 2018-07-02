package com.liuyang.xdr.test;


import java.io.IOException;
import java.util.Map;

import com.liuyang.ftp.FtpClient;
import com.liuyang.log.Logger;
import com.liuyang.thread.FixedThreadPool;
import com.liuyang.xdr.client.FtpFileListClient;
import com.liuyang.xdr.protocol.Channel;
import com.liuyang.xdr.server.receiver.XDRReceiveServer;
import com.liuyang.xdr.server.xdrfile.XDRFileServer;
import com.liuyang.xdr.udf.UDF;

public class XDRFileServerTest {
	private final static Logger logger = Logger.getLogger(XDRFileServerTest.class);
	
	public static long length = 0;
	
	public static long getFreeMemery() {
		return Runtime.getRuntime().freeMemory();
	}
	
	public static long getTotalMemery() {
		return Runtime.getRuntime().totalMemory();
	}
	
	public static long getUsedMemery() {
		return getTotalMemery() - getFreeMemery();
	}
	
	static XDRFileServer server;
	static XDRReceiveServer handler;
	static FtpFileListClient client;
	static FtpClient ftpClient;
	static Map<String, String> fileInfo;
	static FixedThreadPool<Integer> thradpool;
	static Channel sender;
	
	public static void main(String[] args) throws IOException {
		server = new XDRFileServer(38002);
		server.start();
		
		//client = new FtpFileListClient("localhost", 38002);
		//client.login("liuyang", "lcservis");
		
		//sender = client.openReceiver("100", "|");
		//sender.setName("client - send data");
		
		//thradpool = new FixedThreadPool<Integer>(2);
		
		logger.debug(String.format(
				 "System: usedMemery >> %.2fMB, freeMemery >> %.2fMB, totalMemery >> %.2fMB"
				,((double) (UDF.getUsedMemery())) / 1024 / 1024
				,((double) (UDF.getFreeMemery())) / 1024 / 1024
				,((double) (UDF.getTotalMemery())) / 1024 / 1024
			));
		
		
	}

}

package com.liuyang.xdr.test;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.Callable;

import com.liuyang.data.util.LongValue;
import com.liuyang.ftp.FtpClient;
import com.liuyang.ftp.FtpClient.Mode;
import com.liuyang.ftp.FtpClientException;
import com.liuyang.log.Logger;
import com.liuyang.thread.FixedThreadPool;
import com.liuyang.thread.SimpleThreadPool;
import com.liuyang.xdr.client.FtpFileListClient;
import com.liuyang.xdr.protocol.Channel;
import com.liuyang.xdr.server.receiver.XDRReceiveServer;
import com.liuyang.xdr.server.xdrfile.XDRFileServer;
import com.liuyang.xdr.udf.Meta;

public class FtpFileListTest {
	private final static Logger logger = Logger.getLogger(FtpFileListTest.class);
	
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
	static SimpleThreadPool<Integer> threadpool;
	static Channel sender;
	
	public static void main(String[] args) throws IOException {
		//server = new XDRFileServer(38002);
		//server.start();
		//logger.debug(System.currentTimeMillis());
		//logger.debug(new File("d:\\LTE_GZ_YDGZG00475_103000256042_20180618210002.txt").lastModified());
		//System.exit(0);
		client = new FtpFileListClient("192.168.9.1", 38002);
		client.login("liuyang", "lcservis");
		
		sender = client.openReceiver("100", "|");
		sender.setName("client - send data");
		
		//thradpool = new FixedThreadPool<Integer>(2);
		
		logger.debug("4 usedMemery >> " + ((double) (getUsedMemery())) / 1024 / 1024);
		logger.debug("5 freeMemery >> " + ((double) (getFreeMemery())) / 1024 / 1024);
		
		test1();
	}
	

	public void test2() {
		
		try {
			//logger.debug(Meta.getOneXDRFile("17700", "100", "21", "0"));
			//server = new XDRFileServer(38002);
			//server.start();
			
			client = new FtpFileListClient("192.168.9.1", 38002);
			
			client.login("liuyang", "lcservis");
			fileInfo = client.getOneXDRFile("17706", "100", "8", "0");
			ftpClient = new FtpClient();
			
			
			client.logout();
			client.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			
		}
	}
	
	public static void test1() {

		threadpool = new SimpleThreadPool<Integer>(5);
		for (int i = 0; i <= 10; i++) {
			//new Handler(sender, client.getOneXDRFile("17706", "100", "8", "0"));
			threadpool.submit(new Handler(sender, client,  "17714", "100", "8", "0"));
			//new Handler(sender, client,  "17708", "100", "8", "0").start();
		}
		threadpool.start();
		
	}
	
	private static class Handler implements Callable<Integer> {

		private Channel sender;
		private FtpFileListClient receiver;
		private String date;
		private String table;
		private String hour;
		private String status;
		
		public Handler(Channel sender, FtpFileListClient receiver, String date, String table, String hour, String status) {
			this.sender = sender;
			this.receiver = receiver;
			this.date = date;
			this.table = table;
			this.hour = hour;
			this.status = status;
			
		}
		
		@Override
		protected void finalize() {
			this.sender = null;
			this.receiver = null;
			date = null;
			table = null;
			hour = null;
			status = null;
		}
		
		public Integer call() {
			Map<String, String> fileInfo = receiver.getOneXDRFile(date, table, hour, status);
			if (fileInfo == null) return 0;
			if (fileInfo.size() == 0) return 0;
			FtpClient ftpClient;
			try {
				LongValue length = new LongValue();
				ftpClient = new FtpClient();
				if ("succful".equals(fileInfo.get("STATUS"))) {
					if (ftpClient.connect(fileInfo.get("HOST"), Integer.valueOf(fileInfo.get("PORT")), fileInfo.get("USER"), fileInfo.get("PASS"))) {
						//ftpClient.copyRemoteFileToLocal(fileInfo.get("FILEPATH"), "d:/" + fileInfo.get("FILENAME"), Mode.OVERWRITE);
						//logger.debug(ftpClient.lines(fileInfo.get("FILEPATH")).count());
						System.gc();
						long u1 = getUsedMemery();
						logger.debug("1 usedMemery >> " + ((double) (u1)) / 1024 / 1024);
						double s = System.nanoTime();
						ftpClient.lines(fileInfo.get("FILEPATH")).forEach(t -> {
							final int strLen = t.getBytes().length;
							length.compute(x -> x + strLen + 1);
							//length += t.getBytes().length + 1;
							if (this.sender != null) this.sender.writeUTF(t);
							t = null;
						});
						double u = (System.nanoTime() - s) / 1000000;
						double l = ((double) (length.getLong()))/ 1024 / 1024;
						logger.debug(String.format("Speed: %.3fMB/s", l / u * 1000));
						//logger.debug("1 bufferSize >> " + XDRReceiveServer.BUFFER.size());
						
						logger.debug("2 fileLength >> " + l);
						//System.gc();
						logger.debug("3 incrementMemery >> " + ((double) (getUsedMemery() - u1)) / 1024 / 1024);
						//XDRReceiveServer.BUFFER.clear();

						ftpClient.closeStream();
						ftpClient.quit();
					}
				} 
				ftpClient.close();
			
			} catch (FtpClientException e) {
				e.printStackTrace();
			} finally {
				//
				//logger.debug("4 usedMemery >> " + ((double) (getUsedMemery())) / 1024 / 1024);
				//logger.debug("5 freeMemery >> " + ((double) (getUsedMemery())) / 1024 / 1024);
				ftpClient = null;
				System.gc();
				logger.debug("4 usedMemery >> " + ((double) (getUsedMemery())) / 1024 / 1024);
				logger.debug("5 freeMemery >> " + ((double) (getUsedMemery())) / 1024 / 1024);

			}
			return 0;
		}
		
	}
	
	public static void download(Channel sender, Map<String, String> fileInfo) {
		if (fileInfo == null) return;
		if (fileInfo.size() == 0) return;
		FtpClient ftpClient;
		try {
			LongValue length = new LongValue();
			ftpClient = new FtpClient();
			if ("succful".equals(fileInfo.get("STATUS"))) {
                sender.setName("client - send data");
				if (ftpClient.connect(fileInfo.get("HOST"), Integer.valueOf(fileInfo.get("PORT")), fileInfo.get("USER"), fileInfo.get("PASS"))) {
					//ftpClient.copyRemoteFileToLocal(fileInfo.get("FILEPATH"), "d:/" + fileInfo.get("FILENAME"), Mode.OVERWRITE);
					//logger.debug(ftpClient.lines(fileInfo.get("FILEPATH")).count());
					System.gc();
					long u1 = getUsedMemery();
					logger.debug("1 usedMemery >> " + ((double) (u1)) / 1024 / 1024);
					double s = System.nanoTime();
					ftpClient.lines(fileInfo.get("FILEPATH")).forEach(t -> {
						final int strLen = t.getBytes().length;
						length.compute(x -> x + strLen + 1);
						//length += t.getBytes().length + 1;
						sender.writeUTF(t);
						t = null;
					});
					double u = (System.nanoTime() - s) / 1000000;
					double l = ((double) (length.getLong()))/ 1024 / 1024;
					logger.debug(String.format("Speed: %.3fMB/s", l / u * 1000));
					//logger.debug("1 bufferSize >> " + XDRReceiveServer.BUFFER.size());
					
					logger.debug("2 fileLength >> " + l);
					System.gc();
					logger.debug("3 incrementMemery >> " + ((double) (getUsedMemery() - u1)) / 1024 / 1024);
					//XDRReceiveServer.BUFFER.clear();

					ftpClient.closeStream();
					ftpClient.quit();
				}
			} 
			ftpClient.close();
		
		} catch (FtpClientException e) {
			e.printStackTrace();
		} finally {
			System.gc();
			logger.debug("4 usedMemery >> " + ((double) (getUsedMemery())) / 1024 / 1024);
			logger.debug("5 freeMemery >> " + ((double) (getUsedMemery())) / 1024 / 1024);
			ftpClient = null;


		}
	}

}

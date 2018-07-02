package com.liuyang.xdr.protocol;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.Properties;
import java.util.function.Consumer;

import com.liuyang.log.Logger;
import com.liuyang.xdr.parser.BaseParser;

/**
 * 通道
 * @author liuyang
 *
 */
public class Channel {
	private final static Logger logger = Logger.getLogger(Channel.class);
	
	public static enum Mode {
		UPLINK,
		DOWNLINK
	}
	
	private String name;
	private String host = null;
	private int port = 0;
	private boolean isRunning = false;
	private boolean enableListen = false;
	private Socket channelClient;
	private Writer writer;
	private Reader reader;
	private Properties conf;
	private Mode mode;
	private Consumer<? super Channel> action;
	private Channel self = this;
	private Handler handler = null;

	
	public Channel(Socket client, Mode mode) {
		if (client == null) throw new NullPointerException("parameter client is null.");
		
		this.channelClient = client;
		this.mode = mode;
		this.host = client.getInetAddress().getHostName();
		this.port = client.getPort();
		this.conf = new Properties();
		this.isRunning = true;
		//this.handler.start();
		try {
			this.channelClient.setOOBInline(false);
		} catch (SocketException e) {
			e.printStackTrace();
		}
	}
	
	public Channel(Socket client) {
		this(client, Mode.DOWNLINK);
	}
	
	public Channel(Socket client, String name) {
		this(client, Mode.DOWNLINK);
		this.name = name;
	}
	
	public Channel(String host, int port, String name) throws IOException {
		this(new Socket(host, port), Mode.DOWNLINK);
		this.name = name;
	}
	
	public Channel(Socket client, boolean enableListen) {
		this(client, Mode.DOWNLINK);
		this.enableListen = enableListen;
	}

	protected void finalize() {
		if (conf != null) conf.clear();
		action = null;
		conf = null;
		channelClient = null;
		enableListen = false;
		host = null;
		handler = null;
		mode = null;
		port = 0;
		reader = null;
		self = null;
		writer = null;
	}
	

	/**
	 * 启用监听
	 */
	public synchronized final void enableListener() {
		enableListen = true;
	}
	
	/**
	 * 停止监听
	 */
	public synchronized final void disableListener() {
		enableListen = false;
	}
	
	public synchronized final String getHost() {
		return host;
	}
	
	public synchronized final int getPort() {
		return port;
	}
	
	public synchronized final Writer getWriter() throws IOException {
		if (channelClient == null) throw new NullPointerException("the client is null.");
		if (writer == null) writer = new Writer(channelClient.getOutputStream());
		return writer;
	}
	
	public synchronized final Reader getReader() throws IOException {
		if (channelClient == null) throw new NullPointerException("the client is null.");
		if (reader == null) reader = new Reader(channelClient.getInputStream());
		return reader;
	}
	
	public synchronized final boolean isClosed() {
		if (channelClient == null) return true;
		return channelClient.isClosed();
	}
	
	public synchronized final boolean isConnected() {
		boolean retval = true;
		if (channelClient == null) retval = false;
		if (channelClient.isClosed()) retval = false;
		try {
			if (retval) channelClient.sendUrgentData(0xFF);
		} catch (IOException e) {
			System.out.println("channel[" + name + "] isConnected osscurs exception");
			e.printStackTrace();
			retval = false;
		} 
		return retval;
	}
	
	public synchronized final boolean isRunning() {
		return isRunning;
	}
	
	public synchronized final void writeUTF(String str) {
		try {
			getWriter().writeUTF(str);
			getWriter().flush();
		} catch (IOException e) {
			e.printStackTrace();
			close();
		} finally {
			
		}
		
	}
	
	public synchronized final String readUTF() {
		try {
			return getReader().readUTF();
		} catch (IOException e) {
			//e.printStackTrace();
			close();
		}
		return null;
	}
	
	/**
	 * 绕过监听程序进行处理
	 * @param action
	 */
	public synchronized void handle(Consumer<? super Channel> action) {
		action.accept(self);
		//this.handler = new Handler();
		//this.handler.start();
	}
	
	/**
	 * 启用监听程序。
	 * 需要使用enableListen()来启用监听功能，否则不会执行action。
	 * 
	 * @param action
	 */
	public synchronized void listen(Consumer<? super Channel> action) {
		if (action != null) this.action = action;
		if (enableListen == false) return;
		if (isRunning) {
			isRunning = false;
			try {
				if (handler != null) this.handler.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				this.isRunning = true;
			}
		}
		this.handler = new Handler();
		this.handler.start();
		
	}
	
	public synchronized final Mode mode() {
		return mode;
	}
	
	public synchronized final String getName() {
		return name;
	}
	
	public synchronized void close() {
		try {
			if (channelClient != null) {
				isRunning = false;
				
				if (handler != null) {
					//System.out.println("channel [" + name + "] stop handler...");
					//handler.join(); // 等待handler处理完毕
					//handler.destroy();
					//handler.stop();
				}
				channelClient.close();
				System.out.println("channel [" + name + "] is closed.");
			}
		} catch (IOException e) {
			e.printStackTrace();
		//} catch (InterruptedException e) {
		//	e.printStackTrace();
		} finally {
			handler = null;
			channelClient = null;
		}
	}
	
	public synchronized final String getName(String name) {
		return name;
	}
	
	public synchronized final void setName(String name) {
		this.name = name;
	}
	
	private final class Handler extends Thread {
		@Override
		public synchronized void run() {
			setName(name);
			logger.debug("channel [" + name + "] listen handler start");
			// 如未处理运行状态，则会退出监听线程
			while(isRunning()) {
				// 如果未启动监听，则不执行action
				if (enableListen) action.accept(self);
				if (!isRunning()) {
					logger.debug("channel [" + name + "] listen breaken. isRunning=" + isRunning);
					break;
				}
				if (isClosed()) {
					logger.debug("channel [" + name + "] listen breaken. isClosed=" + isClosed());
					break;
				}
			}
			logger.debug("channel [" + name + "] listen handler stop");
		}
	}
}

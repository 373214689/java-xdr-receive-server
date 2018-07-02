package com.liuyang.xdr.protocol.server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import com.liuyang.log.Logger;
import com.liuyang.xdr.protocol.Channel;

public class BaseServer {
	private final static Logger logger = Logger.getLogger(BaseServer.class);
	private final static long IDEL_WAIT_TIME = 1000L;
	
	private List<Channel> container = new ArrayList<Channel>();
	
	private boolean isRunning = false;
	private String name;
	private String host;
    private int port;
    private int limit;
    private ServerSocket server;
    private Handler handler;
    private Function<? super BaseServer, ? super Channel> action;
    private BaseServer self = this;

    
    public BaseServer(String host, int port, int limit) {
    	this.host = host;
    	this.port = port;
    	this.limit = limit;
    }
    
    public BaseServer(String host, int port) {
    	this.host = host;
    	this.port = port;
    }
    
    public BaseServer(int port) {
    	this.port = port;
    }
    
    protected void finalize() {
    	stop();
    	container.clear();
    	container = null;
    	isRunning = false;
    	port = 0;
    	limit = 0;
    	server = null;
    	handler = null;
    	action = null;
    }
    
    public Socket accept() throws IOException {
    	// 
    	return server.accept();
    }
    
	public String getHost() {
		return host;
	}
	
	public int getPort() {
		return port;
	}
    
    public void handle(Consumer<? super BaseServer> action) {
    	action.accept(this);
    }
    
    public boolean isRunning() {
    	return isRunning;
    }
    
    public void listen(Function<? super BaseServer, ? super Channel> action) {
    	this.action = action;
    }
    
    public void start() throws IOException {
    	server = new ServerSocket(port, limit);
		//host = server.getInetAddress().getHostAddress().toString();
    	//host = server.getLocalSocketAddress().toString();
    	host = host == null ? InetAddress.getLocalHost().getHostAddress() : host;
    	port = server.getLocalPort();
    	name = String.format("%s [%s:%d] - %d", name, host, port, System.currentTimeMillis());
    	isRunning = server.isBound(); // 如果绑定端口成功则表示server已在运行
    	handler = new Handler();
    	handler.setName(name);
    	handler.start();
    	
    }
    
    private void beforeStop() {
    	Channel channel = null;
		try {
			channel = new Channel(new Socket(host, port), "server stop socket");
			channel.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			channel = null;
		}
    }
    public void stop() {
    	if (server == null) return;
    	
		isRunning = false;
		beforeStop();
    		
    	try {
    		handler.join();
    		//XDRFileSession.stopAll();
    		container.forEach(Channel::close);
    		logger.debug("server [" + name + "] All sessions of the server has been closed.");
			server.close();
			logger.debug("server [" + name + "] has been closed.");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			action = null;
			server = null;
			handler = null;
		}
    }
    
    public String getName() {
    	return name;
    }
    
    public void setName(String name) {
    	this.name = name;
    }
    
	private class Handler extends Thread {
		@Override
		public void run() {
			logger.debug("server [" + name + "] listen handler start.");
			while (isRunning) {
				if (action != null) {
					Channel retval = (Channel) action.apply(self);
					if (retval != null) container.add(retval);
					retval = null;
				} else {
					// 如果不设置处理动作，则线程进行等待
					try {
						sleep(IDEL_WAIT_TIME);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				if (isRunning == false) break;
				if (server.isClosed()) break;
			}
			logger.debug("server [" + name + "] listen handler end.");
		}
	}
}

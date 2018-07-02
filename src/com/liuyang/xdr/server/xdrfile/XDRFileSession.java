package com.liuyang.xdr.server.xdrfile;


import java.io.IOException;
import java.net.Socket;
import java.util.List;

import com.liuyang.data.util.Row;
import com.liuyang.log.Logger;
import com.liuyang.xdr.protocol.server.BaseServer;
import com.liuyang.xdr.protocol.server.Request;
import com.liuyang.xdr.protocol.server.Response;
import com.liuyang.xdr.protocol.server.Session;
import com.liuyang.xdr.server.receiver.XDRReceiveServer;
import com.liuyang.xdr.udf.Meta;
import com.liuyang.xdr.util.Node;

public class XDRFileSession extends Session {
	private final static Logger logger = Logger.getLogger(XDRFileSession.class);
	
	
	//public static List<XDRFileSession> container = new ArrayList<XDRFileSession>();
	public static XDRFileSession accept(Socket client, boolean enableListen) throws IOException {
		XDRFileSession session = new XDRFileSession(client, enableListen);
		//session.start();
		//session.enableListen = enableListen;
		//container.add(session);
		return session;
	}
	


	
	private synchronized final static Node<String, String> parse(String line) {
		if (line == null) return null;
		if (line.equals("\r\n") || line.equals("\n")) return null;
		Node<String, String> retval = new Node<String, String>();
		int split = line.indexOf(": ");
		retval.key = line.substring(0, split).trim().toUpperCase();
		retval.value = line.substring(split + 1).trim();
		return retval;
	}
	
	/**
	 * 请求结构：
	 * <br>METHOD : 方法名
	 * <br>COOKIE : session生成的随机序列, 用于验证
	 * <br>$PARAM1 : 参数1
	 * <br>$PARAMN : 参数N
	 * @return
	 */
	private synchronized final static Request getRequest(Session session) {
		Request request = null;
		Node<String, String> node = null;
		request = new Request(session);
		while ((node = parse(session.readUTF())) != null) {
			request.setAttribute(node.key, node.value);
		}
		logger.debug("session [" + session.getName() + "] getRequest: " + request);
		//if (!request.containsKey("METHOD"))
		//	throw new IllegalArgumentException("Illegal request, has not a value of METHOD.");
		//if (!request.containsKey("COOKIE"))
		//	throw new IllegalArgumentException("Illegal request, has not a value of COOKIE.");
		return request;
	}
	
	private synchronized final static void sendResponse(Response response) {
		Session session = response.getSession();
		logger.debug("session [" + session.getName() + "] sendResponse: " + response);
		for(Object key : response.keys()) {
			session.writeUTF(key + ": " + response.get(key) + "\r\n");
		}
		session.writeUTF("\r\n");
	}
	
	private long contectionStartTime;
	private String username;
	private boolean isAuthentication = false;
	private XDRReceiveServer receiver = null;
	
	private XDRFileSession(Socket client, boolean enableListen) {
		super(client, enableListen, XDRFileSession::getRequest, XDRFileSession::sendResponse);
		
		contectionStartTime = System.currentTimeMillis();
		setName("xdr file session - " + contectionStartTime);

	}
	
	@Override
	protected void finalize() {
		close();
		contectionStartTime = 0;
		username = null;
		isAuthentication = false;
	}
	
	public synchronized final boolean login(Request request, Response response) {
		//System.out.println("login " + request + " " + response);
		String[] names = {"USER", "PASS"};
    	boolean retval = checkRequest("login", request, response, names);
		if (retval == false) return retval;
		
		String user = String.valueOf(request.getAttribute("USER"));
		String pass = String.valueOf(request.getAttribute("PASS"));
		if (user.length() > 0) {
			switch (user) {
				case "liuyang": {
					retval = "lcservis".equals(pass);
				}
				case "test2018" : {
					retval = "lcservis".equals(pass);
				}
			}
			isAuthentication = retval;
			username = user;
			response.setHeader("MESSAGE", "200 " + username + " login succful");
		} else {
			response.setHeader("MESSAGE", "513 please specify the USER");
		}
		return retval;
	}
	
	public synchronized final boolean logout(Request request, Response response) {
		String[] names = { };
    	boolean retval = checkRequest("logout", request, response, names);
    	if (retval == false) return retval;
    	if (retval = (isAuthentication && username != null)) {
			response.setHeader("MESSAGE", "200 " + username + " logout succful");
			isAuthentication = false;
			username = null;
		} else {
			response.setHeader("MESSAGE", "500 please login at frist");
			retval = false;
		}
		return retval;
	}
	
	public synchronized final boolean noop(Request request, Response response) {
        return true;
	}
	
	public synchronized final boolean select(Request request, Response response) {
		String[] names = {"SQL"};
    	boolean retval = checkRequest("select", request, response, names);
    	if (retval == false) return retval;
		if (isAuthentication && username != null) {
			 List<Row> result = Meta.select(String.valueOf(request.getAttribute("SQL")));
			if (result != null) {
				response.setHeader("RESULT", String.valueOf(result));
			} else {
				response.setHeader("STATUS", "failure");
				response.setHeader("MESSAGE", "500 there is no xdr file to load.");
			}
		} 
		return retval;
	}
	
	public synchronized final boolean update(Request request, Response response) {
		String[] names = {"SQL"};
    	boolean retval = checkRequest("update", request, response, names);
    	if (retval == false) return retval;
		if (retval = (isAuthentication && username != null)) {
			int result = Meta.update(String.valueOf(request.getAttribute("SQL")));
			response.setHeader("RESULT", String.valueOf(result));
			//response.setHeader("MESSAGE", "500 there is no xdr file to load.");
		} 
		return retval;
	}
	
	public synchronized final boolean selectxdr(Request request, Response response) {
		String[] names = {"DATESTAMP", "TABLEID", "HOURS", "STATUS"};
    	boolean retval = checkRequest("selectxdr", request, response, names);
    	if (retval == false) return retval;
    	if (retval = (isAuthentication && username != null)) {
			Row fileInfo = Meta.getOneXDRFile(
					 request.getString("DATESTAMP")
					,request.getString("TABLEID")
					,request.getString("HOURS")
					,request.getString("STATUS")
			);
			if (retval = (fileInfo != null)) {
				response.setHeader("PROTOCOL", "ftp");
				response.setHeader("FILENAME", fileInfo.getString("file_name"));
				response.setHeader("FILEPATH", fileInfo.getString("file_path"));
				response.setHeader("HOST", fileInfo.getString("ftp_server_host"));
				response.setHeader("PORT", fileInfo.getInteger("ftp_server_port"));
				response.setHeader("USER", fileInfo.getString("ftp_user_name"));
				response.setHeader("PASS", fileInfo.getString("ftp_pass_word"));
				response.setHeader("MESSAGE", "200 " + fileInfo.getString("file_path") + " has been get");
			} else {
				response.setHeader("MESSAGE", "500 there is no xdr file to load.");
			}
		} else {
			response.setHeader("MESSAGE", "500 please login at frist.");
		}
		return retval;
	}
	
	public synchronized final boolean updatexdr(Request request, Response response) {
		String[] names = {"FILENAME", "STATUS", "FILELENGTH", "STARTTIME", "ENDTIME"};
    	boolean retval = checkRequest("selectxdr", request, response, names);
    	if (retval == false) return retval;
    	if (retval = (isAuthentication && username != null)) {
			Row fileInfo = Meta.getOneXDRFile(
					 request.getString("DATESTAMP")
					,request.getString("TABLEID")
					,request.getString("HOURS")
					,request.getString("STATUS")
			);
			if (retval = (fileInfo != null)) {
				response.setHeader("PROTOCOL", "ftp");
				response.setHeader("FILENAME", fileInfo.getString("file_name"));
				response.setHeader("FILEPATH", fileInfo.getString("file_path"));
				response.setHeader("HOST", fileInfo.getString("ftp_server_host"));
				response.setHeader("PORT", fileInfo.getInteger("ftp_server_port"));
				response.setHeader("USER", fileInfo.getString("ftp_user_name"));
				response.setHeader("PASS", fileInfo.getString("ftp_pass_word"));
				response.setHeader("MESSAGE", "200 " + fileInfo.getString("file_path") + " has been get");
			} else {
				response.setHeader("MESSAGE", "500 there is no xdr file to load.");
			}
		} else {
			response.setHeader("MESSAGE", "500 please login at frist.");
		}
    	return retval;
	}
	
    public synchronized final boolean openReceiver(Request request, Response response) {
    	String[] names = {"CLASS", "TABLEID", "DELIMITER", "MODE"};
    	boolean retval = checkRequest("openReceiver", request, response, names);
    	if (retval == false) return retval;
    	if (retval = (isAuthentication && username != null)) {
			if (receiver != null) {
				response.setHeader("MESSAGE", "300 Receiver has been opened.");
			} else {
				receiver = XDRReceiveServer.createReceiver(
					 request.getString("TABLEID")
					,request.getString("DELIMITER")
				);
				
			}
			if (receiver != null) {
				response.setHeader("HOST", receiver.getHost());
				response.setHeader("PORT", receiver.getPort());
				response.setHeader("MESSAGE", "200 Receiver open succful.");
			} else {
				response.setHeader("STATUS", "failure");
				response.setHeader("MESSAGE", "200 Receiver open failure.");
			}

		} else {
			response.setHeader("MESSAGE", "500 please login at frist.");
		}
		names = null;
		return retval;
    }
    
    public synchronized final boolean closeReceiver(Request request, Response response) {
    	String[] names = { };
    	boolean retval = checkRequest("closeReceiver", request, response, names);
    	if (retval == false) return retval;
    	if (retval = (isAuthentication && username != null)) {
    		if ((retval = receiver != null)) {
    			XDRReceiveServer.destroyReceiver(receiver);
				response.setHeader("MESSAGE", "200 Receiver close succful.");
    		} else {
    			response.setHeader("MESSAGE", "500 please open Receiver at first.");
    		}
    		receiver = null;
		} else {
			response.setHeader("MESSAGE", "500 please login at frist.");
		}
		return retval;
    }
    
    @Override
    public synchronized final void close() {
    	super.close();
    	// 原则上不允许session直接关闭receive server
    	//XDRReceiveServer.destroyReceiver(receiver);
    }
	
}

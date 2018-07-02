package com.liuyang.xdr.client;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.liuyang.log.Logger;
import com.liuyang.xdr.protocol.Channel;
import com.liuyang.xdr.util.Node;

public class FtpFileListClient extends BaseClient {
	private final static Logger logger = Logger.getLogger(FtpFileListClient.class);

	String cookie;
	Channel channel;
	boolean isContected = false;
	
	public FtpFileListClient(String host, int port) throws IOException {
		channel = new Channel(new Socket(host, port), "xdr file list client - " + System.currentTimeMillis());
		//isContected = (client != null);
		Map<String, String> response = getResponse();
		cookie = response.get("COOKIE");
		
		//logger.debug(response);
		//logger.debug(cookie);
		
	}
	
	private synchronized Node<String, String> parse(String line) {
		if (line == null) return null;
		if (line.equals("\r\n") || line.equals("\n")) return null;
		Node<String, String> retval = new Node<String, String>();
		//logger.debug(line);
		line = line.replace("\r", "").replace("\n", "");
		int split = line.indexOf(": ");
		retval.key = line.substring(0, split).trim().toUpperCase();
		retval.value = line.substring(split + 1).trim();
		return retval;
	}
	
	private synchronized Map<String, String> getResponse() {
		Map<String, String> response = new HashMap<String, String>();
		Node<String, String> node = null;
		while ((node = parse(channel.readUTF())) != null) {
			response.put(node.key, node.value);
		}
		logger.debug("client.getresponse: " + response);
		if (!response.containsKey("METHOD"))
			throw new IllegalArgumentException("Illegal request, has not a value of METHOD.");
		if (!response.containsKey("COOKIE"))
			throw new IllegalArgumentException("Illegal request, has not a value of COOKIE.");
		//if (!cookie.equals(request.get("COOKIE")))
			//throw new IllegalArgumentException("Illegal request, the value COOKIE can not match.");0
		return response;
		
	}
	
	private synchronized Map<String, String> createRequest(String method) {
		Map<String, String> header = new LinkedHashMap<String, String>();
		header.put("METHOD", method);
		header.put("COOKIE", cookie);
		return header;
	}
	
	public synchronized void sendRequest(Map<String, String> request) {
		logger.debug("client.setRequest: " + request);
		for (String key : request.keySet()) {
			channel.writeUTF(key + ": " + request.get(key) + "\r\n");
		}
		channel.writeUTF("\r\n");
	}
	
	public synchronized void login(String username, String password) {
		Map<String, String> header = createRequest("login");
		header.put("USER", username);
		header.put("PASS", password);
		sendRequest(header);
		Map<String, String> response = getResponse();
	}
	
	public synchronized void logout() {
		Map<String, String> header = createRequest("logout");
		sendRequest(header);
		Map<String, String> response = getResponse();
	}
	
	public synchronized void noop() {
		Map<String, String> header = createRequest("noop");
		sendRequest(header);
	}
	
	/**
	 * 
	 * @param datestamp
	 * @param tableid
	 * @param hour
	 * @param status
	 * @return
	 */
	public synchronized final Map<String, String> getOneXDRFile(String datestamp, String tableid, String hour, String status) {
		Map<String, String> header = createRequest("selectxdr");
		header.put("DATESTAMP", datestamp);
		header.put("TABLEID", tableid);
		header.put("HOURS", hour);
		header.put("STATUS", status);
		sendRequest(header);
		return getResponse();
	}
	
	public synchronized final Map<String, String> updateXDRFileStatus(String datestamp, String fileName, String status) {
		Map<String, String> header = createRequest("updatexdr");
		header.put("DATESTAMP", datestamp);
		header.put("FILENAME", fileName);
		header.put("STATUS", status);
		//header.put("STARTTIME", value);
		//header.put("FILELENGTH", value);
		//header.put("ENDTIME", value);
		sendRequest(header);
		return getResponse();
	}
	
	public synchronized final Channel openReceiver(String tableid, String delimiter) {
		Map<String, String> header = createRequest("openReceiver");
		header.put("CLASS", "");
		header.put("TABLEID", tableid);
		header.put("DELIMITER", delimiter);
		header.put("MODE", "TEXT");
		sendRequest(header);
		Map<String, String> response = getResponse();
		try {
			return new Channel(new Socket(response.get("HOST"), Integer.valueOf(response.get("PORT"))), false);
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public synchronized Map<String, String> closeReceiver() {
		Map<String, String> header = createRequest("closeReceiver");
		sendRequest(header);
		return getResponse();
	}
	
	public synchronized void close() {
		isContected = false;
		if (channel != null) {
			channel.close();
		}
	}
}

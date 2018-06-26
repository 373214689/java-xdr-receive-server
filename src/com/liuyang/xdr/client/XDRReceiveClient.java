package com.liuyang.xdr.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.liuyang.xdr.protocol.Channel;
import com.liuyang.xdr.protocol.Channel.Mode;
import com.liuyang.xdr.util.Node;

public class XDRReceiveClient {
	String cookie;
	Socket client;
	Channel channel;
	
	boolean isContected = false;
	
	public XDRReceiveClient(String host, int port) throws IOException {
		client = new Socket(host, port);
		isContected = (client != null);
		Map<String, String> response = getResponse();
		cookie = response.get("COOKIE");
		
		//System.out.println(response);
		//System.out.println(cookie);
		
	}
	
	private DataInputStream getReader() throws IOException {
		return new DataInputStream(client.getInputStream());
	}
	
	private DataOutputStream getWriter() throws IOException {
		return new DataOutputStream(client.getOutputStream());
	}
	
	private boolean ready() throws IOException {
		if (client == null) return false;
		return client.getInputStream().available() > 0;
	}
	
	private Node<String, String> parse(String line) {
		if (line == null) return null;
		if (line.equals("\r\n") || line.equals("\n")) return null;
		Node<String, String> retval = new Node<String, String>();
		int split = line.indexOf(": ");
		retval.key = line.substring(0, split).trim().toUpperCase();
		retval.value = line.substring(split + 1).trim();
		return retval;
	}
	
	private Map<String, String> getResponse() {
		String line;
		DataInputStream reader;
		Map<String, String> response = new HashMap<String, String>();
		try {
			reader = getReader();
			Node<String, String> node = null;
			while ((node = parse(reader.readUTF())) != null) {
				response.put(node.key, node.value);
			}
			System.out.println("client.getresponse: " + response);
			if (!response.containsKey("METHOD"))
				throw new IllegalArgumentException("Illegal request, has not a value of METHOD.");
			if (!response.containsKey("COOKIE"))
				throw new IllegalArgumentException("Illegal request, has not a value of COOKIE.");
			//if (!cookie.equals(request.get("COOKIE")))
				//throw new IllegalArgumentException("Illegal request, the value COOKIE can not match.");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			line = null;
			reader = null;
		}
		return response;
		
	}
	
	private Map<String, String> createRequest(String method) {
		Map<String, String> header = new LinkedHashMap<String, String>();
		header.put("METHOD", method.toUpperCase());
		header.put("COOKIE", cookie);
		return header;
	}
	
	public void sendRequest(Map<String, String> request) {
		DataOutputStream writer;
		try {
			System.out.println("client.setRequest: " + request);
			writer = new DataOutputStream(client.getOutputStream());
			for (String key : request.keySet()) {
				writer.writeUTF(key + ": " + request.get(key) + "\r\n");
			}
			writer.writeUTF("\r\n");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			writer = null;
		}
	}
	
	public void login(String username, String password) {
		Map<String, String> header = createRequest("LOGIN");
		header.put("USER", username);
		header.put("PASS", password);
		sendRequest(header);
		Map<String, String> response = getResponse();
	}
	
	public void logout() {
		Map<String, String> header = createRequest("LOGOUT");
		sendRequest(header);
		Map<String, String> response = getResponse();
	}
	
	public void noop() {
		Map<String, String> header = createRequest("NOOP");
		sendRequest(header);
	}
	
	public Channel pipe() throws IOException {
		Map<String, String> header = createRequest("PIPE");
		sendRequest(header);
		Map<String, String> response = getResponse();
		if ("succful".equals(response.get("STATUS"))) {
			String host = response.get("HOST");
			int port = Integer.valueOf(response.get("PORT"));
			channel = new Channel(new Socket(host, port), Mode.UPLINK);
			return channel;
		}
		return null;
	}
	
	/**
	 * 
	 * @param datestamp
	 * @param tableid
	 * @param hour
	 * @param status
	 * @return
	 */
	public Map<String, String> getOneXDRFile(String datestamp, String tableid, String hour, String status) {
		Map<String, String> header = createRequest("SELECTXDR");
		header.put("DATESTAMP", datestamp);
		header.put("TABLEID", tableid);
		header.put("HOURS", hour);
		header.put("STATUS", status);
		sendRequest(header);
		return getResponse();
	}
	
	public void close() {
		try {

			isContected = false;
			if (client != null) {
				client.setKeepAlive(false);
				client.shutdownInput();
				client.shutdownOutput();
				client.close();
			
				System.out.println("client close.");
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}

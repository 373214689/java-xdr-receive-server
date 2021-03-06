package com.liuyang.xdr.server.receiver;


import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.liuyang.xdr.parser.XDRParser100;
import com.liuyang.xdr.protocol.Channel;
import com.liuyang.xdr.protocol.server.BaseServer;
import com.liuyang.xdr.util.LinkedList;

public class XDRReceiveServer extends BaseServer {
	public static int RECEIVER_START_PORT = 48000;
	public static int RECEIVER_PORT_RANGE = 1000;
	private static Map<String, XDRReceiveServer> RECEIVER_MAP = new HashMap<String, XDRReceiveServer>();
	
	public final static XDRReceiveServer createReceiver(String tableid, String delimiter) {
		int min = RECEIVER_START_PORT, max = RECEIVER_START_PORT + RECEIVER_PORT_RANGE;
		if (!RECEIVER_MAP.containsKey(tableid)) {
			if (RECEIVER_MAP.size() == 0) {
				RECEIVER_MAP.put(tableid, new XDRReceiveServer(min, tableid));
				try {
					RECEIVER_MAP.get(tableid).start();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				for(int i = min; i < max; i++) {
					for(XDRReceiveServer svr : RECEIVER_MAP.values()) {
						if (svr.getPort() != i) {
							RECEIVER_MAP.put(tableid, new XDRReceiveServer(i, tableid));
							try {
								RECEIVER_MAP.get(tableid).start();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
				}
			}

		}
		return RECEIVER_MAP.get(tableid);
	}
	
	public synchronized final static void destroyReceiver(XDRReceiveServer receiver) {
		if (receiver == null) return;
		if (RECEIVER_MAP.containsKey(receiver.tableId)) {
			RECEIVER_MAP.remove(receiver.tableId).stop();
		}
	}
	
	public synchronized final static void destroyAllReceiver() {
		Iterator<Entry<String, XDRReceiveServer>> itor = RECEIVER_MAP.entrySet().iterator();
		while(itor.hasNext()) {
			Entry<String, XDRReceiveServer> element = itor.next();
			element.getValue().stop();
			itor.remove();
		}
		itor = null;
	}
	
	private XDRParser100 parser = new XDRParser100();
	private String tableId;
	
	public XDRReceiveServer(int port, String tableId) {
		super(port);
		this.tableId = tableId;
		setName("xdr receive server (" + tableId + ")");
	}
	
	@Override
	public void start() throws IOException {
		super.start();
		//setName(String.format("xdr receive server[%s:%d] - %d", getHost(), getPort(), System.currentTimeMillis()));
		parser.start();
		listen(svr -> {
			try {
				return new XDRReceiveSession(svr.accept());
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		});
	}
	
	@Override
	public void stop() {
		super.stop();
		parser.stop();
	}
	
    private  class XDRReceiveSession extends Channel {
    	
    	public XDRReceiveSession (Socket client) {
    		super(client, true);
    		
    		setName("xdr receive session (" + tableId + ") - " + System.currentTimeMillis());
    		listen(channel -> {
    			//channel.readUTF();
    			String line = channel.readUTF();
    			if (line != null) parser.parse(line);
			    //BUFFER.push(channel.readUTF());
    		});

    	}
	}
}

package com.liuyang.xdr.server.xdrfile;

import java.io.IOException;

import com.liuyang.xdr.protocol.server.BaseServer;


public class XDRFileServer extends BaseServer {

	public XDRFileServer(String host, int port) {
		super(host, port);
		setName("xdr file server");
		//setName(String.format("xdr file server[%s:%d] - %d", getHost(), getPort(), System.currentTimeMillis()));
	}
	
	public XDRFileServer( int port) {
		super(port);
		setName("xdr file server");
		//setName(String.format("xdr file server[%s:%d] - %d", getHost(), getPort(), System.currentTimeMillis()));
	}
	
	@Override
	public void start() throws IOException {
		super.start();
		
		listen(svr -> {
			try {
				return XDRFileSession.accept(svr.accept(), svr.isRunning());
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		});
	}
}

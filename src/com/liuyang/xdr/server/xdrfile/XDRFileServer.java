package com.liuyang.xdr.server.xdrfile;

import java.io.IOException;

import com.liuyang.xdr.protocol.server.BaseServer;
import com.liuyang.xdr.server.receiver.XDRReceiveServer;


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
	
	@Override
	public void stop() {
		super.stop();
		// 主服务线程关闭后，需要关闭附属线程（即由服务会话开启的从属线程）
		XDRReceiveServer.destroyAllReceiver();
	}
}

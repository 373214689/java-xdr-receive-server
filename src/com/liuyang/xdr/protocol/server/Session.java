package com.liuyang.xdr.protocol.server;

import java.lang.reflect.Method;
import java.net.Socket;
import java.util.HashMap;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;

import com.liuyang.log.Logger;
import com.liuyang.xdr.protocol.Channel;
import com.liuyang.xdr.server.xdrfile.XDRFileSession;

public class Session extends Channel{
	private final static Logger logger = Logger.getLogger(Session.class);
	
	protected Class<?> clazz = this.getClass();
	protected Class<?> types[] = {Request.class, Response.class};
	
	private Properties conf;
	private String cookie;
	private Session self = this;
	
	private Function<? super Session, Request> requestPaser;
	private Consumer<? super Response> responseSender;
	
	public Session(Socket client, boolean enableListen, 
			Function<? super Session, Request> requestParser, Consumer<? super Response> responseSender) 
	{
		super(client, enableListen);
		this.requestPaser = requestParser;
		this.responseSender = responseSender;
		this.cookie = getHost() + ":" + getPort() + "-" + System.currentTimeMillis() + "-" + System.nanoTime();
		this.conf = new Properties();
		if (enableListen) {
			responseWellcom();
		}
		// 如果指定了request的解析函数，则开始监听request
		if (requestPaser != null) {
			listen(channel -> {
				Request request = request();
	            if (request == null) return;
				if (!request.containsKey("METHOD")) return;
				if (!request.containsKey("COOKIE")) return;
				if (!cookie.equals(request.getProperty("COOKIE"))) return;
				handle(request);
			});
		}

	}
	
	private synchronized final Request request() {
		if (requestPaser != null) {
			return requestPaser.apply(self);
		}
		return null;
	}
	
	private synchronized final Response response(String method) {
		Response retval = new Response(this);
		retval.setMethod(method);
		retval.setStatus("succful");
		retval.setHeader("COOKIE", cookie);
		return retval;
	}
	
	private synchronized final void send(Response response) {
		if (responseSender != null) {
			responseSender.accept(response);
		}
		/*for(Object key : response.keys()) {
			writeUTF(key + ": " + response.get(key) + "\r\n");
		}
		writeUTF("\r\n");*/
	}
	
	private synchronized final void responseWellcom() {
		send(response("welcom"));
	}
	
	public synchronized final void responseNoSuchMethod(Request request) {
		Response retval = response("unknown");
		retval.setStatus("failure");
		retval.setHeader("MESSAGE", "404 No such method " + request.getMethod());
		send(retval);
	}
	
	public synchronized final void responseBadRequest(Request request) {
		Response retval = response(request.getMethod());
		retval.setStatus("failure");
		retval.setHeader("MESSAGE", "401 Bad Request " + request);
		send(retval);
	}
	
	public synchronized final boolean checkRequest(String method, Request request, Response response, String... names) {
		if (method == null) return false;
		String message = "";
		boolean retval = false;
		if (!(retval = method.equals(request.getMethod()))) {
			message = "404 Illegal request,(" + request.getMethod() + ") METHOD is not " + method + ".";
		}
		if (names == null) {
			retval = true;
		} else if (names.length == 0) {
			retval = true;
		} else {
			for(String name : names) {
				if (retval == false) break; // 如果检测到错误，跳出循环
				if (!request.containsKey(name)) {
					if (retval == false) break;
					message = "404 Illegal request, has not a value of " + name + ".";
					retval = false;
				}
			}
		}
		if (!retval) System.err.println(message);
		response.setHeader("MESSAGE", message);
		message = null;
		return retval;
	}
	
	public synchronized final String cookie() {
		return cookie;
	}
	
	public synchronized final void setAttribute(Object key, Object value) {
		conf.put(key, value);
	}
	
	public synchronized final Object getAttribute(Object key) {
		return conf.get(key);
	}
	
	protected synchronized final void handle(Request request) {
		Method method = null;
		String name = null;
		Response response = null;
		/*
		 * Method 提供关于类或接口上单独某个方法（以及如何访问该方法）的信息。所反映的方法可能是类方法或实例方法（包括抽象方法）。
		 */
		try {
			name = request.getMethod();
			if(name!=null && !"".equals(name)){
				method = getMethod(name);
				if (method != null) {
					try {
						
						response = response(name);
						//System.out.println(method.getName() + "  " + request + " " + response);
						boolean retval = (boolean) method.invoke(self, request, response);
						response.setStatus(retval ? "succful" : "failure");
						send(response); // 向客户端发送响应消息
						/*
						 * Method类中 invoke()方法的作用 对带有指定参数的指定对象调用由此 Method
						 * 对象表示的基础方法。个别参数被自动解包，以便与基本形参相匹配，基本参数和引用参数都随需服从方法调用转换。
						 */
					} catch (Exception ex) {
						logger.error("----调用方法[" + name +"]失败----");
						ex.printStackTrace();
						responseBadRequest(request);
					}
				}
			}else{
				logger.error("----未指定mehtod参数----");
			}
		} catch (NoSuchMethodException e) {
			logger.error("----未找到名为[" + name + "]的方法----");
			e.printStackTrace();
			responseNoSuchMethod(request);
		}
	}
	
	protected HashMap<String, Method> methods = new HashMap<String, Method>();
	protected synchronized final Method getMethod(String name) throws NoSuchMethodException {

		/***********************************************************************
		 * synchronized锁
		 */
		synchronized (methods) {
			Method method = (Method) methods.get(name);
			if (method == null) {
				method = clazz.getMethod(name, types);
				/*
				 * //***Class 类中getMethod()方法 返回一个 Method 对象，它反映此 Class
				 * 对象所表示的类或接口的指定公共成员方法。name 参数是一个
				 * String，用于指定所需方法的简称。parameterTypes 参数是按声明顺序标识该方法形式参数类型的 Class
				 * 对象的一个数组。如果 parameterTypes 为 null，则按空数组处理。 如果 name 是 "<init>"
				 * 或 "<clinit>"，则将引发 NoSuchMethodException。否则，要反映的方法由下面的算法确定（设 C
				 * 为此对象所表示的类）：
				 * 
				 * 在 C 中搜索任一匹配的方法。如果找不到匹配的方法，则将在 C 的超类上递归调用第 1 步算法。 如果在第 1
				 * 步中没有找到任何方法，则在 C 的超接口中搜索匹配的方法。如果找到了这样的方法，则反映该方法。 在 C
				 * 类中查找匹配的方法：如果 C 正好声明了一个具有指定名称的公共方法并且恰恰有相同的形式参数类型，则它就是反映的方法。如果在
				 * C 中找到了多个这样的方法，并且其中有一个方法的返回类型比其他方法的返回类型都特殊，则反映该方法；否则将从中任选一个方法。
				 */
				methods.put(name, method);
			}
			return (method);
		}

	}

}

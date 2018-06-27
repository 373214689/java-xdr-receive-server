package com.liuyang.xdr.parser;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Callable;

import com.liuyang.data.util.Row;
import com.liuyang.thread.SimpleThreadPool;
import com.liuyang.xdr.udf.UDF;

public class XDRCache {
    private static final long BUFFER_LIMIT_SIZE = 10 * 1024 * 1024;
    private static final String CACHE_ROOT_PATH = "/root/project/xdr/cache";
	
	private static Map<Row, Buffer> container = new HashMap<Row, Buffer>();
	
	private static Map<String, XDRCache> caches = new HashMap<String, XDRCache>();
	
	public synchronized final XDRCache get(String tableId) {
		if (!caches.containsKey(tableId)) caches.put(tableId, new XDRCache(tableId));
		return caches.get(tableId);
	}
	
	private String tableId;
	private XDRCache(String tableId) {
		this.tableId = tableId;
	}
	
	public static void push(Row key, String value) {
		synchronized(container) {
			Buffer buffer = null;
			//System.out.println(key);
			if (!container.containsKey(key)) container.put(key, new Buffer(key));
			
			container.get(key).push(value);
		}
	}
	
	public static void flushRemains() {
		synchronized(container) {
			for(Row key : container.keySet()) {
				container.get(key).flush();
			}
			container.clear();
		}
	}
	
	private static SimpleThreadPool<Integer> threadpool = new SimpleThreadPool<Integer>(10);
	
	private static class Buffer {
		LinkedList<String> buffer;
		
		Row partitions;
		
		File childrenPath;
		
		File bufferPath;
		String templementFileName;
		
		int count = 0;
		
		private long length = 0;
		private long counter = 0;
		
		public Buffer(Row partitions) {
			this.partitions = partitions;
			this.buffer = new LinkedList<String>();

			String basePath = String.join("/", partitions.schema().getChildren().stream().limit(1)
					.map(field -> field.getFieldName())
					.map(field -> field + "=" + partitions.getString(field))
					.toArray(size -> new String[size]));
			templementFileName = String.join("_", partitions.schema().getChildren().stream()
					.map(field -> field.getFieldName())
					.map(field -> partitions.getString(field))
					.toArray(size -> new String[size]));
			this.childrenPath = new File(CACHE_ROOT_PATH, partitions.schema().getFieldName());
			this.bufferPath = new File(childrenPath, basePath);
			if (!bufferPath.exists()) bufferPath.mkdirs();
			
			basePath = null;
		}
		
		public synchronized void push(String e) {
			synchronized(buffer) {
				buffer.addLast(e);
				length += e.getBytes().length;
				counter++;
				if (length >= BUFFER_LIMIT_SIZE) {
					flush();
				}
			}
		}

		private synchronized void flush() {
			count++;
			File target = new File(
					bufferPath
				   ,String.format("%s_%04d_%06d.%s", templementFileName, count, System.nanoTime() % 65536, "txt"));
			threadpool.submit(new Handler(counter, target));
			threadpool.start();
			length = 0;
			counter = 0;
			target = null;
		}
		
		private class Handler implements Callable<Integer> {
			long lines;
			File target;
		
			public Handler (long lines, File target) {
				this.lines = lines;
				this.target = target;
			}
			
			protected void finalize() {
				lines = 0;
				target = null;
			}
			
			private String pop() {
				synchronized(buffer) {
					String retval = null;
					if (!buffer.isEmpty() && lines > 0) {
						retval = buffer.removeFirst();
						lines--;
					}
					return retval;
				}
			}
			
			private boolean write(File target) {
				
				boolean retval = false;
				FileWriter writer = null;
				try {
					target.getParentFile().mkdirs();
					writer = new FileWriter(target, true);
					String line = null;
					while((line = pop()) != null) {
						writer.write(line);
						writer.write("\n");
					}
					writer.close();
					retval = true;
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					writer = null;
				}
				return retval;
			}

			public Integer call() {
				if (write(target) && target.exists()) {
					System.out.println(target + " write. length = " + target.length());
					System.gc(); // 释放内存
					System.out.println(String.format(
							 "System: memery(%.2f%%), usedMemery >> %.2fMB, freeMemery >> %.2fMB, totalMemery >> %.2fMB"
							,((double) UDF.getUsedMemery()) / ((double) UDF.getTotalMemery()) * 100
							,((double) (UDF.getUsedMemery())) / 1024 / 1024
							,((double) (UDF.getFreeMemery())) / 1024 / 1024
							,((double) (UDF.getTotalMemery())) / 1024 / 1024
						));
				}
				return 0;
			}
		}

	}
	
	private class Convertor {
		
	}


}

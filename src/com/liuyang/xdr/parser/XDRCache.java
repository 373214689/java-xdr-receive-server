package com.liuyang.xdr.parser;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import com.liuyang.data.util.Row;
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
	
	private static class Buffer {
		LinkedList<String> buffer;
		
		Row partitions;
		
		File childrenPath;
		
		File bufferPath;
		String templementFileName;
		
		int count = 0;
		
		private long length = 0;
		
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
				if (length >= BUFFER_LIMIT_SIZE) {
					flush();
				}
			}
		}
		
		private boolean write(File target) {
			synchronized(buffer) {
				boolean retval = false;
				FileWriter writer = null;
				try {
					target.getParentFile().mkdirs();
					writer = new FileWriter(target);
					while(!buffer.isEmpty()) {
						String line = buffer.removeFirst();
						if (line != null) {
							writer.write(line);
							writer.write("\n");
						}
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
		}
			
		private synchronized void flush() {
			File target = new File(
					bufferPath
				   ,String.format("%s_%04d_%06d.%s", templementFileName, count, System.nanoTime() % 65536, "txt"));
			if (write(target) && target.exists()) {
				length = 0;
				System.out.println(target + " write. length = " + target.length());
				System.gc(); // 释放内存
				System.out.println(String.format(
						 "System: usedMemery >> %.2fMB, freeMemery >> %.2fMB, totalMemery >> %.2fMB"
						,((double) (UDF.getUsedMemery())) / 1024 / 1024
						,((double) (UDF.getFreeMemery())) / 1024 / 1024
						,((double) (UDF.getTotalMemery())) / 1024 / 1024
					));
			}
			target = null;
		}

	}
	
	private class Convertor {
		
	}


}

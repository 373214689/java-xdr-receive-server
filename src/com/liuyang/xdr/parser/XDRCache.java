package com.liuyang.xdr.parser;

import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import com.liuyang.data.util.Row;
import com.liuyang.data.util.Schema;
import com.liuyang.log.Logger;
import com.liuyang.thread.SimpleThreadPool;
import com.liuyang.xdr.udf.ORC;

public class XDRCache {
	private final static Logger logger = Logger.getLogger(XDRCache.class);
	
	/** 设定Buffer的缓存长度，值越大，需要消耗的内存就会越多 */
    private static final long BUFFER_LIMIT_SIZE = 128 * 1024 * 1024;
    private static final String CACHE_ROOT_PATH = "/root/project/xdr/cache";
    private static final String CONVERTOR_ROOT_PATH = "/home/xdr/cache";
	/** 每个Cache有一个线程池，负责使用线程来写入文件 */
	private static SimpleThreadPool<Integer> BUFFER_THREADPOOL = new SimpleThreadPool<Integer>(10);
	/** 文件转换线程池 */
	private static SimpleThreadPool<Integer> CONVERTOR_THREADPOOL = new SimpleThreadPool<Integer>(10);
	
	private static Map<Row, Buffer> container = new HashMap<Row, Buffer>();
	
	private static Map<String, XDRCache> caches = new HashMap<String, XDRCache>();
	
	private static LinkedList<Convertor> convertors = new LinkedList<Convertor>();
	
	public synchronized final XDRCache get(String tableId) {
		if (!caches.containsKey(tableId)) caches.put(tableId, new XDRCache(tableId));
		return caches.get(tableId);
	}
	
	private String tableId;
	private XDRCache(String tableId) {
		this.tableId = tableId;
	}
	
	public final static void push(Schema schema, Row partitions, String value) {
		synchronized(container) {
			//logger.debug(key);
			if (!container.containsKey(partitions)) container.put(partitions, new Buffer(schema, partitions));
			container.get(partitions).push(value);
		}
	}
	
	public final static void flushRemains() {
		synchronized(container) {
			Iterator<Entry<Row, Buffer>> itor = container.entrySet().iterator();
			while (itor.hasNext()) {
				Entry<Row, Buffer> element = itor.next();
				Buffer tmp = element.getValue();
				tmp.flush();
				itor.remove();
				tmp = null;
				element = null;
			}
			itor = null;
		}
	}
	
	public final static void flushExpireds(long mills) {
		synchronized(container) {
			long currentTime = System.currentTimeMillis();
			Iterator<Entry<Row, Buffer>> itor = container.entrySet().iterator();
			while (itor.hasNext()) {
				Entry<Row, Buffer> element = itor.next();
				Buffer tmp = element.getValue();
				if (tmp.lastModifiedTime + mills <= currentTime) {
					tmp.flush();
					itor.remove();
				}
                tmp = null;
                element = null;
			}
			itor = null;
			currentTime = 0;
		}
	}
	
	public final static void pushExpiredFile(long mills) {
		synchronized(convertors) {
			logger.debug("pushExpiredFile start to check expired file. size = " + convertors.size());
			long currentTime = System.currentTimeMillis();
			Iterator<Convertor> itor = convertors.iterator();
			while (itor.hasNext()) {
				Convertor element = itor.next();
				if (element.source.lastModified() + mills < currentTime) {
					logger.debug("pushExpiredFile: " + element.source + " will be convert. length = " + element.source.length());
					element.submit();
					itor.remove();
				}
				element = null;
			}
			itor = null;
			currentTime = 0;
		}
	}
	

	
	private final static class Buffer {

		LinkedList<String> buffer;
		
		Row partitions;
		
		File childrenPath;
		
		File bufferPath;
		String templementFileName;
		Schema schema;
		
		int count = 0;
		
		private long length = 0;
		private long counter = 0;
		private long lastModifiedTime = 0;
		private long bufferLimitSize = 0;
		private File current;
		
		public Buffer(Schema schema, Row partitions) {
			this.schema = schema;
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
		
		@Override
		protected void finalize() {
			if (buffer != null) buffer.clear();
			count = 0;
			length = 0;
			counter = 0;
			childrenPath = null;
			partitions = null;
			templementFileName = null;
			lastModifiedTime = 0;
			current = null;
		}
		
		public void push(String e) {
			synchronized(buffer) {
				if (current == null) getOrCreateFile();
				buffer.addLast(e);
				length += e.getBytes().length;
				counter++;
				lastModifiedTime = System.currentTimeMillis();
				if (length >= bufferLimitSize) {
					flush();
				}
			}
		}
		
		private int bufferSize() {
			synchronized(buffer) {
				return buffer.size();
			}
		}
		
		private synchronized void getOrCreateFile() {
			//File retval = null;
			if (bufferPath == null) throw new NullPointerException();
			if (bufferPath.exists()) {
				//String fileNameHeader = String.format("%s_%04d", templementFileName, )
				//File[] list = bufferPath.listFiles();
				File[] list = bufferPath.listFiles(new FileFilter() {
					@Override
					public boolean accept(File pathname) {
						return pathname.getName().startsWith(templementFileName);
					}
				});
				for (int i = 0, length = list.length; i < length; i++) {
					if (list[i].length() < BUFFER_LIMIT_SIZE) {
						current = list[i];
						break;
					}
				}
				if (current == null) {
					count++; // 表示新建了一个文件
					current = new File(bufferPath, String.format("%s_%04d_%d.%s"
							,templementFileName
							,count
							,System.currentTimeMillis()
							,"txt"
					));
					bufferLimitSize = BUFFER_LIMIT_SIZE;
					logger.debug(current + " will be created.");
				} else {
					bufferLimitSize = BUFFER_LIMIT_SIZE - current.length();
					logger.debug(current + " has been found. length = " + current.length());
				}
				

				list = null;
			}
		}

		private synchronized void flush() {
			if (current == null) {
				logger.debug("can not get or create file in " + bufferPath);
			} else {
				// 建立处理线程
				BUFFER_THREADPOOL.submit(new Handler(counter, current));
				BUFFER_THREADPOOL.start();
				current = null;
			}
			// 无论是否处理，均重置计数器
			length = 0;
			counter = 0;
		}
		
		private class Handler implements Callable<Integer> {
			long lines;
			File target;
		
			public Handler (long lines, File target) {
				this.lines = lines;
				this.target = target;
			}
			
			protected void finalize() {
				this.lines = 0;
				this.target = null;
			}
			
			private String pop() {
				synchronized(buffer) {
					String retval = null;
					if (lines > 0) {
						retval = buffer.isEmpty() ? null : buffer.removeFirst();
						lines--;
					}
					return retval;
				}
			}
			
			private boolean write(File target) {
				
				boolean retval = false;
				FileWriter writer = null;
				String line = null;
				try {
					target.getParentFile().mkdirs();
					writer = new FileWriter(target, true);
					while((line = pop()) != null) {
						writer.write(line);
						writer.write("\n");
					}
					writer.close();
					retval = true;
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					line = null;
					writer = null;
				}
				return retval;
			}

			public Integer call() {
				long totalLines = lines;
				if (write(target) && target.exists()) {
					logger.debug("%s writed. length = %d, buffer.size=%d, lines=%d"
							,target
							,target.length()
							,bufferSize()
							,totalLines
					);
					synchronized(convertors) {
						if (current != null) {
						    long cnt = convertors.stream().filter(e -> e.source.equals(target) && e.partitions.equals(partitions)).count();
						    if (cnt <= 0) {
						    	convertors.addLast(new Convertor(schema, partitions, target));
						    	logger.debug(target + " has been added to convertors.");
						    }
						}
					}
                    // System.gc(); // 释放内存
				}
				totalLines = 0;
				return 0;
			}
		}

	}
	
	private static class Convertor {
		private File source;
		private Row partitions;
		private Schema schema;
		
        public Convertor(Schema schema, Row partitions, File source) {
        	this.partitions = partitions;
        	this.source = source;
        	this.schema = schema;
        	
        }
        
        protected void finalize() {
        	this.partitions = null;
        	this.source = null;
        }
        
        /**
         * 提交转换请求
         */
        public void submit() {
        	CONVERTOR_THREADPOOL.submit(new Handler());
        	CONVERTOR_THREADPOOL.start();
        }
        
        public class Handler implements Callable<Integer> {
        	
			@Override
			public Integer call() throws Exception {
				String fileName = source.getName();
				File target = new File(CONVERTOR_ROOT_PATH, fileName + "_zlib.orc");
				//ORC.convertDataSchemaORC(dataSchema)
				ORC.convertTextToOrc(source.getAbsolutePath(), target.getAbsolutePath(), ORC.convertDataSchemaORC(schema));
				if (target.exists()) source.delete();
				return 0;
			}
        	
        }
	}




}

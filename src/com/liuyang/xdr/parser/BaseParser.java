package com.liuyang.xdr.parser;


import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;

import com.liuyang.data.util.Row;
import com.liuyang.data.util.Schema;
import com.liuyang.log.Logger;
import com.liuyang.xdr.udf.UDF;
//import com.liuyang.xdr.util.LinkedList;

public class BaseParser {
	private final static Logger logger = Logger.getLogger(BaseParser.class);
	
	private LinkedList<String> buffer = null;
	
	private boolean isRunning = false;
	private Schema schema;
	private Schema defaultPartSchema;
	private Schema partSchema;
	private Row row;
	private Row defaultPartition;
	private Handler handler;
	private volatile long counter = 0;
	private XDRCache cache;
	
	private Timer memeryControllorTimer;
	
	public BaseParser(Schema schema) {
		this.schema = schema;
		this.row = schema.createRow();
		this.handler = new Handler();
		this.buffer = new LinkedList<String>();
		this.defaultPartSchema = Schema.createStruct(schema.getFieldName());
		this.defaultPartSchema.addField(Schema.create("p_default", "int"));
		this.defaultPartition = defaultPartSchema.createRow();
		this.defaultPartition.setValue("p_default", 0);
		this.memeryControllorTimer = new Timer("MemeryControllorTimer");
		this.memeryControllorTimer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				double usedMemery = UDF.getUsedMemery();
				double freeMemery = UDF.getFreeMemery();
				double taotalMemery = UDF.getTotalMemery();
				double memeryUseagePersent = usedMemery / taotalMemery * 100;
				logger.debug(
						"System: handler: {counter: %d}, buffer: {size : %d}, memery: {usage: %.2f%%, used: %.2fMB, free: %.2fMB, total: %.2fMB}"
						,counter
						,size()
						,memeryUseagePersent
						,usedMemery / 1024 / 1024
						,freeMemery / 1024 / 1024
						,taotalMemery / 1024 / 1024
				);
				// 清除缓存，并将数据缓冲到磁盘
				XDRCache.flushExpireds(60000);
				// 清除到期的文件（即该文件长期未操作）
				XDRCache.pushExpiredFile(180000);
				// 释放内存，当使用内存量大于10%的时候
				if (memeryUseagePersent > 10.00) System.gc(); 
				usedMemery = 0.00;
				freeMemery = 0.00;
				taotalMemery = 0.00;
				memeryUseagePersent = 0.00;
			}
		}, 0, 10000);
	}
	
	protected void finalize() {
		if (buffer != null) buffer.clear();
		if (memeryControllorTimer != null) memeryControllorTimer.cancel();
		schema = null;
		partSchema = null;
		defaultPartition = null;
		row = null;
		handler = null;
		buffer = null;
		counter = 0;
		isRunning = false;
		memeryControllorTimer = null;
	}
	
	public synchronized void setPartitions(Schema partSchema) {
		if (partSchema != null) this.partSchema = partSchema;
	}
	
	public synchronized Schema getSchema() {
		return schema;
	}
	
	public synchronized void start() {
		isRunning = true;
		handler.start();
	}
	
	public synchronized void stop() {
		synchronized(buffer) {
			while(!buffer.isEmpty()) {
				logger.debug("Sbuffer.size = %d, counter = %d", size(), counter);
				///System.out.println("buffer.size = " + buffer.size() + " counter = " + counter);
				/*for(String s : buffer) {
					System.out.println(buffer.size() + "  " + s);
				}*/
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			XDRCache.flushRemains();
			isRunning = false;
			memeryControllorTimer.cancel();
			//logger.debug("Sbuffer.size = %d, counter = %d", size(), counter);
		}
		

	}
	
	private int size() {
		synchronized (buffer) {
			return buffer.size();
		}
	}
	
	private void push(String line) {
		synchronized (buffer) {
			buffer.addLast(line);
			
		}
	}
	
	private String pop() {
		synchronized (buffer) {
			return buffer.pop();
		}
	}
	
	public synchronized void parse(String line) {
		push(line);
	}
	
	
	private class Handler extends Thread {
		
		@Override
		public void run() {
			while(isRunning) {
				String line;
				try {
					line = pop();
					if ((line = pop()) != null) {
						counter++;
						//System.out.println(line);
						row.parseLine(line, "|");
						Row partitions = (partSchema != null) ? partSchema.compute(row) : defaultPartition;
						//XDRCache.push(schema, partitions, line);
						XDRCache.push(schema, partitions, line);
						partitions = null;
						//System.out.println(row.parseLine(buffer.pop(), "|"));
					}
				} catch (Exception e) {
					try {
						Thread.sleep(100);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				} finally {
					line = null;
				}
			}
		}
	}
}

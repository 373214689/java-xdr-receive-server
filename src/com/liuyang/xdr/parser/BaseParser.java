package com.liuyang.xdr.parser;

import java.util.LinkedList;

import com.liuyang.data.util.IntValue;
import com.liuyang.data.util.LongValue;
import com.liuyang.data.util.Row;
import com.liuyang.data.util.Schema;

public class BaseParser {
	private LinkedList<String> buffer = null;
	
	private boolean isRunning = false;
	private Schema schema;
	private Schema partSm;
	private Row row;
	private Handler handler;
	private long counter = 0;
	private XDRCache cache;
	
	public BaseParser(Schema schema) {
		this.schema = schema;
		this.row = schema.createRow();
		this.handler = new Handler();
		this.buffer = new LinkedList<String>();
		this.partSm = Schema.createStruct(schema.getFieldName());
		this.partSm.addField(Schema.create("p_day", "int"));
		this.partSm.addField(Schema.create("p_time", "bigint"));
		this.partSm.addField(Schema.create("p_type", "int"));
		
		this.partSm.getField("p_day").setValueCompute(row -> {
			long startdate = row.getLong("startdate") + 28800000000L;
			return new IntValue((int) ((startdate - startdate % 86400000000L) / 86400000000L));
		});
		this.partSm.getField("p_time").setValueCompute(row -> {
			long startdate = row.getLong("startdate") + 28800000000L;
			return new LongValue((startdate - startdate % 60000000L) / 1000);
		});
		this.partSm.getField("p_type").setValueCompute(row -> {
			return new IntValue(row.getInteger("apptype"));
		});
	}
	
	protected void finalize() {
		if (buffer != null) buffer.clear();
		schema = null;
		row = null;
		handler = null;
		buffer = null;
		counter = 0;
		isRunning = false;
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
				System.out.println("buffer.size = " + buffer.size() + " counter = " + counter);
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
			System.out.println("BaseParser.counter=" + counter);
		}
		

	}
	
	public synchronized void parse(String line) {
		buffer.addLast(line);
	}
	
	
	
	private class Handler extends Thread {
		
		@Override
		public void run() {
			while(isRunning) {
				synchronized(buffer) {
					try {
						String line = buffer.removeFirst();
						if (line != null) {
							counter++;
							//System.out.println(line);
							row.parseLine(buffer.pop(), "|");
							XDRCache.push(partSm.compute(row), line);
							//System.out.println(row.parseLine(buffer.pop(), "|"));
						}
					} catch (Exception e) {
						try {
							Thread.sleep(100);
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}
					}

				}
			}
		}
	}
}

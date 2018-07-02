package com.liuyang.xdr.udf;

public class UDF {
	public final static long TIMEZONE_OFFSET_MILLS = 28400000L;
	public final static long DAYSTAMP_MILLS = 86400000L;
	public final static long MINUTES_MILLS = 60000L;
	
	public static long getFreeMemery() {
		return Runtime.getRuntime().freeMemory();
	}
	
	public static long getTotalMemery() {
		return Runtime.getRuntime().totalMemory();
	}
	
	public static long getUsedMemery() {
		return getTotalMemery() - getFreeMemery();
	}
}

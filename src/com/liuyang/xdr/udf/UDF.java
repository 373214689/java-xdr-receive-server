package com.liuyang.xdr.udf;

public class UDF {
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

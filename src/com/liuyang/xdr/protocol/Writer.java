package com.liuyang.xdr.protocol;

import java.io.DataOutputStream;
import java.io.OutputStream;

public class Writer extends DataOutputStream {

	public Writer(OutputStream out) {
		super(out);
	}

}

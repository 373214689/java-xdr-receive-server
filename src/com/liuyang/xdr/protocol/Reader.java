package com.liuyang.xdr.protocol;

import java.io.DataInputStream;
import java.io.InputStream;

public class Reader extends DataInputStream {

	public Reader(InputStream in) {
		super(in);
	}

}

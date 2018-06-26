package com.liuyang.data.util;

public class DataSet {
    Schema schema;

    public DataSet(Schema schema) {
    	this.schema = schema;
    }
    
    public DataSet append(Row row) {
    	
    	return this;
    }
}

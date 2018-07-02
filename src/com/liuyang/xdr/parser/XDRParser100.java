package com.liuyang.xdr.parser;

import com.liuyang.data.util.IntValue;
import com.liuyang.data.util.LongValue;
import com.liuyang.data.util.Schema;
import com.liuyang.xdr.udf.Meta;

public class XDRParser100 extends BaseParser {

	public XDRParser100() {
		super(Meta.getSchema("100"));
		Schema partSchema = Schema.createStruct(getSchema().getFieldName());
		partSchema.addField(Schema.create("p_day", "int"));
		partSchema.addField(Schema.create("p_time", "bigint"));
		partSchema.addField(Schema.create("p_type", "int"));
		
		partSchema.getField("p_day").setValueCompute(row -> {
			long startdate = row.getLong("startdate") + 28800000000L;
			return new IntValue((int) ((startdate - startdate % 86400000000L) / 86400000000L));
		});
		partSchema.getField("p_time").setValueCompute(row -> {
			long startdate = row.getLong("startdate") + 28800000000L;
			return new LongValue((startdate - startdate % 60000000L) / 1000);
		});
		partSchema.getField("p_type").setValueCompute(row -> {
			return new IntValue(row.getInteger("apptype"));
		});
		
		setPartitions(partSchema);
	}

	
}

package com.liuyang.data.util;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import com.liuyang.data.util.Schema.Type;

/**
 * Row
 * <li>2018/6/8 create by liuyang</li>
 * @author liuyang
 * @version 1.0.2
 *
 */
public class Row implements Comparable<Row> {
    
	
	private int id;
	
	//private Row parent;
	
	private Schema schema;
	
	private PrimitveValue[] values;
	
	public Row(Schema schema) {
		this.schema = schema;
		this.values = new PrimitveValue[schema.size()];
	}
	
	@Override
	public synchronized final int compareTo(Row other) {
		if (other == null) throw new NullPointerException();
		if (other.equals(this)) return 0;
		if (other.schema.equals(schema)) {
			return other.schema.getFieldNames().stream()
					    .mapToInt(field -> {return other.get(field).compareTo(get(field));})
					    .sum();
		} else {
			Schema tmpSchema = schema.select(other.schema.getFieldNames());
			return tmpSchema.getFieldNames().stream()
					        .mapToInt(field -> {return other.get(field).compareTo(get(field));})
				            .sum();
		}
	}
    
    /**
     * Checks if the given index is in range.  If not, throws an appropriate
     * runtime exception.  This method does *not* check if the index is
     * negative: It is always used immediately prior to an array access,
     * which throws an ArrayIndexOutOfBoundsException if index is negative.
     * 
     * @param index
     */
    private synchronized final void rangeCheck(int index) {
        if (index >= values.length)
            throw new IndexOutOfBoundsException("index: " + index + ", Size: " + values.length);
    }
    
    /**
     * Check schema
     * @param index
     * @param types
     */
    private final void schemaCheck(int index, Type... types) {
    	Type type = schema.getType(index);
    	if (Arrays.stream(types).filter(e -> e.equals(type)).count() <= 0)
    		throw new IllegalArgumentException(
    				String.format("index: %d, type: %s can not match the %s", 
    						index, 
    						type,  
    						String.join(",", Arrays.stream(types).map(e -> e.getName()).toArray(n -> new String[n]))
    				)
    		);
    }
    
    public synchronized final PrimitveValue get(int index) {
    	rangeCheck(index);
    	return values[index];
    }
    
    public synchronized final PrimitveValue get(String fieldName) {
    	int index = schema.getFieldId(fieldName);
    	return get(index);
    }
    
    public synchronized final boolean getBoolean(int index) {
    	//schemaCheck(index, Type.BOOLEAN);
    	return get(index).getBoolean();
    }
    
    public synchronized final boolean getBoolean(String fieldName) {
    	return get(fieldName).getBoolean();
    }
    
    public final byte[] getBinary(int index) {
    	//schemaCheck(index, Type.BINARY);
    	return get(index).getBinary();
    }
    
    public synchronized final byte[] getBinary(String fieldName) {
    	return get(fieldName).getBinary();
    }
    
    public synchronized final double getDoublue(int index) {
    	//schemaCheck(index, Type.DOUBLE);
    	return get(index).getDouble();
    }
    
    public synchronized final double getDoublue(String fieldName) {
    	return get(fieldName).getDouble();
    }
    
    public synchronized final float getFloat(int index) {
    	//schemaCheck(index, Type.FLOAT);
    	return get(index).getFloat();
    }
    
    public synchronized final float getFloat(String fieldName) {
    	return get(fieldName).getFloat();
    }
    
    public synchronized final int getInteger(int index) {
    	//schemaCheck(index, Type.INTEGER, Type.INT);
    	return get(index).getInteger();
    }
    
    public synchronized final int getInteger(String fieldName) {
    	return get(fieldName).getInteger();
    }
    
    public synchronized final long getLong(int index) {
    	//schemaCheck(index, Type.LONG, Type.BIGINT);
    	return get(index).getLong();
    }
    
    public synchronized final long getLong(String fieldName) {
    	return get(fieldName).getLong();
    }
    
    public synchronized final short getShort(int index) {
    	//schemaCheck(index, Type.SHORT, Type.TINYINT, Type.SMALLINT);
    	return get(index).getShort();
    }
    
    public synchronized final short getShort(String fieldName) {
    	return get(fieldName).getShort();
    }
    
    public synchronized final String getString(int index) {
    	//schemaCheck(index, Type.STRING, Type.VARCHAR);
    	return get(index).getString();
    }
    
    public synchronized final String getString(String fieldName) {
    	return get(fieldName).getString();
    }
    
    //private Object getValue(int index) {
    //	return get(index).getValue();
    //}

    public synchronized final Row set(int index, PrimitveValue value) {
    	rangeCheck(index);
    	if (values[index] != value || values[index] == null) values[index] = value;
    	return this;
    }
    
    /*private Row set(String fieldName, PrimitveValue value) {
    	int index = schema.getFieldId(fieldName);
    	values[index] = value;
    	return this;
    }*/
    
    public synchronized Row setValue(int index, boolean value) {
    	if (values[index] == null) {
    		schemaCheck(index, Type.BOOLEAN);
    		set(index, new BooleanValue(value));
    	} else {
        	get(index).setValue(value);
    	}
    	return this;
    }
    
    public synchronized Row setValue(String fieldName, boolean value) {
    	return setValue(schema.getFieldId(fieldName), value);
    }
    
    public synchronized Row setValue(int index, byte[] value) {
    	if (values[index] == null) {
    		schemaCheck(index, Type.BINARY);
    		set(index, new BinaryValue(value));
    	} else {
        	get(index).setValue(value);
    	}
    	return this;
    }
    
    public synchronized Row setValue(String fieldName, byte[] value) {
    	return setValue(schema.getFieldId(fieldName), value);
    }
    
    public synchronized Row setValue(int index, double value) {
    	if (values[index] == null) {
    		schemaCheck(index, Type.DOUBLE);
    		set(index, new DoubleValue(value));
    	} else {
        	get(index).setValue(value);
    	}
    	return this;
    }
    
    public synchronized Row setValue(String fieldName, double value) {
    	return setValue(schema.getFieldId(fieldName), value);
    }
    
    public synchronized Row setValue(int index, float value) {
    	if (values[index] == null) {
    		schemaCheck(index, Type.FLOAT);
    		set(index, new FloatValue(value));
    	} else {
        	get(index).setValue(value);
    	}
    	return this;
    }
    
    public synchronized Row setValue(String fieldName, float value) {
    	return setValue(schema.getFieldId(fieldName), value);
    }
    
    public synchronized Row setValue(int index, int value) {
    	if (values[index] == null) {
    		schemaCheck(index, Type.INTEGER, Type.INT);
    		set(index, new IntValue(value));
    	} else {
        	get(index).setValue(value);
    	}
    	return this;
    }
    
    public synchronized Row setValue(String fieldName, int value) {
    	return setValue(schema.getFieldId(fieldName), value);
    }
    
    public synchronized Row setValue(int index, long value) {
    	if (values[index] == null) {
    		schemaCheck(index, Type.LONG, Type.BIGINT);
    		set(index, new LongValue(value));
    	} else {
        	get(index).setValue(value);
    	}
    	return this;
    }
    
    public synchronized Row setValue(String fieldName, long value) {
    	return setValue(schema.getFieldId(fieldName), value);
    }
    
    public synchronized Row setValue(int index, short value) {
    	if (values[index] == null) {
    		schemaCheck(index, Type.SHORT, Type.TINYINT, Type.SMALLINT);
    		set(index, new ShortValue(value));
    	} else {
        	get(index).setValue(value);
    	}
    	return this;
    }
    
    public synchronized Row setValue(String fieldName, short value) {
    	return setValue(schema.getFieldId(fieldName), value);
    }
    
    public synchronized Row setValue(int index, String value) {
    	if (values[index] == null) {
    		schemaCheck(index, Type.STRING, Type.VARCHAR);
    		set(index, new TextValue(value));
    	} else {
    		try {
            	get(index).setValue(value);
    		} catch (java.lang.UnsupportedOperationException e) {
    			System.out.println(schema.getField(index) + " " + get(index).getType() + " <> " + index + " : " + value);
    			e.printStackTrace();
    		}

    	}
    	return this;
    }
    
    public synchronized Row setValue(String fieldName, String value) {
    	return setValue(schema.getFieldId(fieldName), value);
    }
    
    private synchronized final String valueIllegalMessage(int index, Type type, Object value, String description) {
    	return String.format("Illegal paramater[%d:%s] <%s> : %s (%s), type is not %s."
    			    ,index
    			    ,schema.getFieldName(index)
    			    ,type
    			    ,value
    			    ,value == null ? "NULL" : value.getClass().getName()
    			    ,description
    		    );
    	//return "Illegal paramater[" + index + ": " + schema.getFieldName(index)+ "]<" + type + ">: " + value + " (" + value.getClass().getName() + "), type is not " + description + ".";
    }
    
    private synchronized final Row setValue(int index, Object value) {
    	Type type = schema.getType(index);
    	switch(type) {
    	case BINARY: {
    		if (value == null) return setValue(index, new byte[] {});
    		if (value instanceof byte[]) return setValue(index, (byte[]) value);
    		throw new IllegalArgumentException(valueIllegalMessage(index, type, value, "byte[]"));
    	}
    	case BOOLEAN: {
    		if (value == null) return setValue(index, false);
    		if (value instanceof Boolean) return setValue(index, (boolean) value);
    		throw new IllegalArgumentException(valueIllegalMessage(index, type, value, "boolean"));
    	}
    	case DOUBLE: {
    		//System.out.println(valueIllegalMessage(index, type, value, "double"));
    		if (value == null) return setValue(index, 0.00d);
    		if (value instanceof Double) return setValue(index, (double) value);
    		throw new IllegalArgumentException(valueIllegalMessage(index, type, value, "double"));
    	}
    	case FLOAT: {
    		if (value == null) return setValue(index, 0.00f);
    		if (value instanceof Float) return setValue(index, (float) value);
    		throw new IllegalArgumentException(valueIllegalMessage(index, type, value, "float"));
    	}
    	case INT:
    	case INTEGER: {
    		if (value == null) return setValue(index, 0);
    		if (value instanceof Integer) return setValue(index, (int) value);
    		if (value instanceof Short)   return setValue(index,  (int) ((short) value));
    		if (value instanceof Long)    return setValue(index,  (int) ((long) value));
    		throw new IllegalArgumentException(valueIllegalMessage(index, type, value, "integer"));
    	}
    	case BIGINT:
    	case LONG: {
    		if (value == null) return setValue(index, 0l);
    		if (value instanceof Long)    return setValue(index, (long) value);
    		if (value instanceof Short)   return setValue(index,  (int) ((short) value));
    		if (value instanceof Integer) return setValue(index, (long) ((int) value));
    		throw new IllegalArgumentException(valueIllegalMessage(index, type, value, "long"));
    	}
    	case TINYINT:
    	case SMALLINT:
    	case SHORT: {
    		if (value == null) return setValue(index, (short) 0);
    		if (value instanceof Short)   return setValue(index, (short) value);
    		if (value instanceof Integer) return setValue(index, (short) ((int) value));
    		if (value instanceof Long)    return setValue(index,  (short) ((long) value));
    		throw new IllegalArgumentException(valueIllegalMessage(index, type, value, "short"));
    	}
    	case VARCHAR:
    	case STRING: {
    		if (value == null) return setValue(index, "");
    		if (value instanceof String) return setValue(index, (String) value);
    		throw new IllegalArgumentException(valueIllegalMessage(index, type, value, "String"));
    	}
    	default:
    		throw new IllegalArgumentException(valueIllegalMessage(index, type, value, "undefined"));
    	}
    }
    
    public synchronized final Row setValue(String fieldName, Object value) {
    	return setValue(schema.getFieldId(fieldName), value);
    }
    
    public synchronized final Row compute() {
    	return this;
    }
    
    public synchronized final Row compute(Function<Row, Row> action) {
    	return action.apply(this);
    }
    
    public synchronized final boolean filter() {
		return true;
    }
    
    public synchronized final boolean filter(Predicate<Row> filter) {
		return filter.test(this);
    }
    
    public synchronized final Row clone() {
    	//if (schema.getParent() == null) return this;
    	Row retval = new Row(schema);
    	for (int i = 0, length = retval.values.length; i < length; i++) {
    		retval.set(i, get(retval.schema.getFieldName(i)).clone());
    	}
    	retval.id = id;
    	return retval;
    }
    
    /**
     * Returns <tt>true</tt> if this list contains the specified element.
     * More formally, returns <tt>true</tt> if and only if this list contains
     * at least one element <tt>e</tt> such that
     * <tt>(o==null&nbsp;?&nbsp;e==null&nbsp;:&nbsp;o.equals(e))</tt>.
     *
     * @param o element whose presence in this list is to be tested
     * @return <tt>true</tt> if this list contains the specified element
     */
    public synchronized final boolean contains(Object o) {
    	return indexOf(o) >= 0;
    }
    
    public synchronized final boolean containsAll(Row other) {
    	for (int i = 0, length = values.length; i < length; i++) {
    		if (indexOf(other.get(i)) == -1) return false;
    	}
    	return true;
    }
    
	public synchronized final boolean equals(Object anObject) {
		if (anObject == this) return true;
		if (anObject == null) return false;
		if (anObject instanceof Row) {
			Row other = (Row) anObject;
			//System.out.println("row equals schema compare " + schema + " == " + other.schema + " >> " + schema.equals(other.schema));
			if (!schema.equals(other.schema)) return false;
			for (int i = 0, length = values.length; i < length; i++) {
				if (!values[i].equals(other.values[i])) return false;
			}
		}
    	return true;
	}
	
	// fill 和 parse方法都会导致id的增加，主要用于行识别。原则上fill效率大于parse
	//    1、fill填充Object[]，将之转换为相应的PrimitveValue对象后储存，
	//       如果Object类型与PrimitveValue限制类型不符则抛出异常。
	//    2、parse填统String[].将之解析为相应的PrimitveValue对象后储存，
	//       如果String无法被解析，则会抛出异常
    public synchronized final Row fill(Object[] datas) {
    	if (datas == null) return this;
    	int length = datas.length > values.length ? values.length : datas.length;
    	for(int i = 0; i < length; i++) {
    		setValue(i, datas[i]);
    	}
    	setId(getId() + 1);
    	return this;
    }
	
    public synchronized final Row parseLine(String line, String delimiter) {
    	if (line == null) return this; // 如果line为空，则不解析
    	int next = 0, pos = 0;
    	String value;
    	for(int i = 0, length = values.length; i < length; i++) {
        	if ((pos = line.indexOf(delimiter, next)) != -1) {
        		value = line.substring(next, pos);
				next = pos + 1;
			} else {
				value = "";
			}
        	setValue(i, schema.getType(i).parseString(value));
        	//set(i, schema.getType(i).parseStringValue(word));
    	}
    	setId(getId() + 1);
    	value = null;
    	return this;
    }
    
    
    // hashCode主要用于HashSet、HashMap、HashTree等对应的Hash表应用。
    @Override
    public synchronized final int hashCode() {
    	if (schema.getType() == Type.STRUCT) {
    		int result = 1987;
    		for(int i = 0, length = values.length; i < length; i++) {
    			int code = values[i] == null ? 0 : values[i].hashCode();
    			result = result * 31 + code;  
    	    }
    		return result;
    	} else {
    		return values[0] == null ? 0 : values[0].hashCode();
    	}
    	
    }
    
    /**
     * Returns the index of the first occurrence of the specified element
     * in this list, or -1 if this list does not contain the element.
     * More formally, returns the lowest index <tt>i</tt> such that
     * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>,
     * or -1 if there is no such index.
     */
    public synchronized final int indexOf(Object o) {
        if (o == null) {
            for (int i = 0, length = values.length; i < length; i++)
                if (values[i] == null)
                    return i;
        } else {
        	for (int i = 0, length = values.length; i < length; i++)
                if (o.equals(values[i]))
                    return i;
        }
        return -1;
    }
    
    // Keys和Values区别：
    //     1、Keys主要用于GroupBy的Key确定，在生成时会调用clone方法。
    //     2、Values主要用于值储存，在生成时将直接引用对象，而不会调用clone方法。
    public synchronized final Row keys(List<String> fieldNames) {
    	//System.out.println(schema.getParent());
    	if (schema.getParent() == null) schema.groupby(fieldNames);
    	Row retval = new Row(schema.getParent());
    	for (int i = 0, length = retval.values.length; i < length; i++) {
    		retval.set(i, get(retval.schema.getFieldName()).clone());
    	}
    	retval.id = id;
    	return retval;
    }
    
    public synchronized final Row keys(Schema schema) {
    	if (schema.getParent() == null) return this;
    	Row retval = new Row(schema.getParent());
    	for (int i = 0, length = retval.values.length; i < length; i++) {
    		retval.set(i, get(retval.schema.getFieldName(i)).clone());
    	}
    	retval.id = id;
    	return retval;
    }
    
    public synchronized final Row keys(String... fieldNames) {
    	return keys(Arrays.asList(fieldNames));
    }
    
    public synchronized final Row keys() {
    	if (schema.getParent() == null) return this;
    	Row retval = new Row(schema.getParent());
    	for (int i = 0, length = retval.values.length; i < length; i++) {
    		retval.set(i, get(retval.schema.getFieldName(i)).clone());
    	}
    	retval.id = id;
    	return retval;
    }
    
    public synchronized final Row values(List<String> fieldNames) {
    	//if (schema.getParent() == null) return this;
    	Row retval = new Row(schema.select(fieldNames));
    	for (int i = 0, length = retval.values.length; i < length; i++) {
    		retval.set(i, get(retval.schema.getFieldName(i)));
    	}
    	retval.id = id;
    	return retval;
    }
    
    public synchronized final Row values(String... fieldNames) {
    	return values(Arrays.asList(fieldNames));
    }
    
    public synchronized final Row values(Schema otherSchema) {
    	return values(otherSchema.getFieldNames());
    }
    
    public synchronized final Row reduce(Row other) {
    	return schema.reduce(this, other);
    }
    
    @Override
    public synchronized final String toString() {
    	return id + ": [" + String.join(", ", Arrays.stream(values).map(PrimitveValue::toString).toArray(n -> new String[n])) + "]";
    }

	public synchronized final int getId() {
		return id;
	}
	
	public synchronized final void setId(int newId) {
		id = newId;
	}
	
	public synchronized final int size() {
		return values.length;
	}
	
	public synchronized final Schema schema() {
		return schema;
	}

}

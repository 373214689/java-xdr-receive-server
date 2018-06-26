package com.liuyang.data.util;

import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collector.Characteristics;
import java.util.stream.Stream;

import com.liuyang.data.util.Schema.Type;

public class Container {
	private TreeMap<Row, Row> values = new TreeMap<Row, Row>();
    
	private Schema schema;
	
	private Schema idSchema = Schema.createStruct("row").addField("id", Type.INT);
	
	
	// container reduce
	private BiFunction<Container, ? super Row, Container> reduceAction = (a, b) -> 
	{ 
	    Schema s1 = a.schema(); // 获取schema
	    Row value = s1.compute(b);  // 调用schema计算过程
	    if (s1.getParent() != null) { // 判断是否拥有groupby字段
		    
			Row key = (Row) value.keys(s1);
		    // 有groupby的数据，需要将groupby的字段做为key来存值，每条key对应一条数据
		    if (!a.containesKey(key)) {
			    a.put(key, value);
		    } else {
		    	
		    	try {
		    		a.get(key).reduce(value); // 调用schema的reduce过程
		    	} catch (Exception e) {
		    		//System.out.println("key: " + key + ", value: " + value + " ,containsKey: " + a.containesKey(key));
		    		e.printStackTrace();
		    		//System.exit(1);
		    	}
			    
		    }
	    } else {
	    	// 没有groupby字段的汇取，则只汇取一条数据
		    if (a.isEmpty()) {
			    a.put((Row) value, value);
		    } else {
			    a.get(a.firstKey()).reduce(value); // 调用schema的reduce过程
		    }
	    }
	    return a;
    };
    
	private BinaryOperator<Container> mergeAction = (a, b) -> 
    {
        // 如果数据模型不一样，则返回a。不进行数据合并。
	    if (a.schema != b.schema) return a;
	    //a.merge(b);
	    /*for (Row key : values.keySet()) {
		a.values.merge(key, values.get(key), (r1, r2) -> {
			return r1.reduce(r2);
		});
	    }*/
		return a;
	};
	
	public Container(Schema schema) 
	{
		this.schema = schema;
		
	}
	
	protected void finalize() {
		
	}

	public final synchronized boolean containesKey(Row key) 
	{
		return values.containsKey(key);
	}
	
	public final boolean isEmpty() {
		return values.isEmpty();
	}
	
	public Row firstKey() 
	{
		return values.firstKey();
	}
	
	public final synchronized Row get(Row key) 
	{
		return values.get(key);
	}
	
	public final synchronized void put(Row key, Row value) 
	{
		values.put(key, value);
	}
	
	/**
	 * Container MapReduce中间计算过程
	 * @return 返回计算过程接口
	 */
	public final BiFunction<Container, ? super Row, Container> reducer() 
	{
        return reduceAction;
		
	}
	/**
	 * Container MapReduce结果合并过程
	 * <br>
	 * 只在并发模式下会调用merge过程
	 * @return 返回合并过程接口
	 */
	public final BinaryOperator<Container> merger() {
		return mergeAction;
	}

	/**
	 * 合并过程，必须使用线程同步
	 * @param other
	 */
	public final synchronized void merge(Container other) 
	{
	    if (other.schema != schema) return;
		for (Row key : values.keySet()) {
			if (other.containesKey(key)) {
			    values.merge(key, values.get(key), (r1, r2) -> {
				    return r1.reduce(r2);
			    });
			}
			//System.out.println("megre: key = " + key + " ,other.containsKey = " + other.containesKey(key));
		}
	}
	
	public final Schema schema() {
		return schema;
	}
	
	public final synchronized int size() {
		return values.size();
	}
	
	public final String toString() {
		return values.toString();
	}
	
	public final Stream<Row> keys() {
		return values.keySet().stream();
	}
	
	public final Stream<Row> values() {
		return values.values().stream();
	}

	/**
	 * 收集器。可以将Strem<Row>收集容器内。
	 * @return
	 */
	public final Collector<Row, Container, Container> collector() {
		Supplier<Container> supplier = () ->
        {
            //return new Container(schema);
        	//System.out.println("call container->collector->supplier");
        	//return new Container(schema);
        	return this;
        };
        BiConsumer<Container, Row> accumulator = (m, t) -> 
        {
        	if (m.isEmpty()) {
        		
        	} else {
        		
        	}
        	Row id = m.idSchema.createRow().set(0, new IntValue(m.size()));
        	//System.out.println("call container->collector->accumulator");
        	m.put(id, t.clone());
        };
        BinaryOperator<Container> combiner = (Container left, Container right) ->
        {
            //left.putAll(right);
        	
        	if (left == right) return left;
        	//System.out.println("call container->collector->combiner");
        	if (left.schema != right.schema) return left;
        	
            for(Row value : right.values.values()) {
            	Row id = left.idSchema.createRow().set(0, new IntValue(left.size()));
            	left.put(id, value);
            }
        	//left.merge(right);
            return left;
        };
        
		return Collector.of(supplier, accumulator, combiner, Characteristics.IDENTITY_FINISH);
	}
	
	/*@Override
	public Supplier<Container> supplier() {
		// TODO Auto-generated method stub
		return ArrayList::new;
	}

	@Override
	public BiConsumer<Container, Row> accumulator() {

		return (a, b) -> {
			if (a.isEmpty()) {
				//a.put(key, value);
			}
		};
	}

	@Override
	public Function<Container, Container> finisher() {
		return Function.identity();
	}

	@Override
	public Set<Characteristics> characteristics() {
		return Collections.unmodifiableSet(EnumSet.of(Collector.Characteristics.IDENTITY_FINISH));
	}

	@Override
	public BinaryOperator<Container> combiner() {
		// TODO Auto-generated method stub
		return null;
	}*/
    
}

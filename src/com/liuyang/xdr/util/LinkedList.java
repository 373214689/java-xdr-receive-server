package com.liuyang.xdr.util;

import java.util.Iterator;
import java.util.function.Consumer;

/**
 * 单向链表
 * <li>规则1：先入先出（FIFO）</li>
 * <li>规则2：线性操作</li>
 * @author liuyang
 * @version 1.0.0
 * @param <T>
 */
public class LinkedList<T> {
    private Node<T> first;
    private Node<T> last;
    private int count;
    
    public LinkedList() {
    	count = 0;
    }
    
    protected void finalize() {
    	first = null;
    	last = null;
    	count = 0;
    }
    
    public synchronized void clear() {
    	while(first != null) {
    		first = first.next;
    		count--;
    	}
    	//System.gc(); // 释放内存
    }
    
    public synchronized void push(T value) {
    	if (first == null) {
    		last = first = new Node<T>(value);
    	} else {
    		last = last.next = new Node<T>(value);
    	}
    	count++;
    }
	
    public synchronized T pop() {
    	Node<T> retval = null;
    	if (first != null) {
    		retval = first;
    		first = first.next;
    		count--;
    	} 
    	return retval == null ? null : retval.value;
    }
    
    public synchronized int size() {
    	return count;
    }
    
    public synchronized boolean isEmpty() {
    	return count == 0;
    }
    
    public synchronized Iterator<T> iterator() {
		return new Itr();
    }

	@SuppressWarnings("hiding")
	private class Node<T> {
		Node<T> next;
		T value;
		
		public Node(T value) {
			this.value = value;
		}
	}
	
	private class Itr implements Iterator<T> {
		Node<T> cursor = first;
		
		@Override
		protected void finalize() {
			cursor = null;
		}
		
		@Override
		public boolean hasNext() {
	    	return cursor != null;
		}

		@Override
		public T next() {
	    	if (first != null) {
	    		cursor = first;
	    		first = first.next;
	    		count--;
	    	} 
			return cursor.value;
		}
	}
}


package com.liuyang.xdr.util;

import java.util.Iterator;

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
    private int size;
    
    public LinkedList() {
    	size = 0;
    }
    
    protected void finalize() {
    	first = null;
    	last = null;
    	size = 0;
    }
    
    public synchronized final void clear() {
    	while(first != null) {
    		first = first.next;
    		size--;
    	}
    	//System.gc(); // 释放内存
    }
    
    public synchronized final void push(T value) {
    	if (first == null) {
    		last = first = new Node<T>(value);
    	} else {
    		last = last.next = new Node<T>(value);
    	}
    	size++;
    }
	
    public synchronized final T pop() {
    	Node<T> retval = null;
    	if (first != null) {
    		retval = first;
    		first = first.next;
    		size--;
    	} 
    	return retval == null ? null : retval.value;
    }
    
    public synchronized final int size() {
    	return size;
    }
    
    public synchronized final boolean isEmpty() {
    	return size == 0;
    }
    
    /**
     * 删除元素，如果匹配到则删除第一个元素。
     * 
     * @param element 待删除的元素。
     * @return 删除成功则返回被删除的元素，失败则返回null。
     */
    public synchronized final T remove(T element) {
    	if (first == null) return null;
    	Node<T> next = first;
    	Node<T> dest = null;
    	while((next != null)) {
    		if (next.value.equals(element)) {
    			// 如果dest == null，则表示next == first
    			// 在dest不为null的情况下（通常是第二个节点起始），则可以使dest.next指向next.next，从而删除next。
    			if (dest ==  null) {
    				first = next.next;
    			} else {
    				dest.next = next.next;
    			}
    			size--;
    			break;
    		}
    		dest = next;
    		next = next.next;
    	}
    	return next.value;
    }
    
    public synchronized final int indexOf(T value) {
    	if (first == null) return -1;
    	Node<T> next = first;
    	int index = 0;
    	while((next != null)) {
    		if (next.value.equals(value)) break;
    		next = next.next;
    		index++;
    	}
    	return next == null ? -1 : index;
    }
    
    public synchronized final boolean contains(T value) {
    	return indexOf(value) != -1;
    }
    
    public synchronized final Iterator<T> iterator() {
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
	    	    size--;
	    	} 
			return cursor.value;
		}
	}
}


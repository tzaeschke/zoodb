package org.zoodb.jdo.stuff;

import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.RandomAccess;
import java.util.Vector;

/*
 * %W% %E%
 *
 * Copyright (c) 2006, Oracle and/or its affiliates. All rights reserved.
 * ORACLE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

/**
 * Resizable-array implementation of the <tt>List</tt> interface.  Implements
 * all optional list operations, and permits all elements, including
 * <tt>null</tt>.  In addition to implementing the <tt>List</tt> interface,
 * this class provides methods to manipulate the size of the array that is
 * used internally to store the list.  (This class is roughly equivalent to
 * <tt>Vector</tt>, except that it is unsynchronized.)<p>
 *
 * The <tt>size</tt>, <tt>isEmpty</tt>, <tt>get</tt>, <tt>set</tt>,
 * <tt>iterator</tt>, and <tt>listIterator</tt> operations run in constant
 * time.  The <tt>add</tt> operation runs in <i>amortized constant time</i>,
 * that is, adding n elements requires O(n) time.  All of the other operations
 * run in linear time (roughly speaking).  The constant factor is low compared
 * to that for the <tt>LinkedList</tt> implementation.<p>
 *
 * Each <tt>ArrayList</tt> instance has a <i>capacity</i>.  The capacity is
 * the size of the array used to store the elements in the list.  It is always
 * at least as large as the list size.  As elements are added to an ArrayList,
 * its capacity grows automatically.  The details of the growth policy are not
 * specified beyond the fact that adding an element has constant amortized
 * time cost.<p>
 *
 * An application can increase the capacity of an <tt>ArrayList</tt> instance
 * before adding a large number of elements using the <tt>ensureCapacity</tt>
 * operation.  This may reduce the amount of incremental reallocation.
 *
 * <p><strong>Note that this implementation is not synchronized.</strong>
 * If multiple threads access an <tt>ArrayList</tt> instance concurrently,
 * and at least one of the threads modifies the list structurally, it
 * <i>must</i> be synchronized externally.  (A structural modification is
 * any operation that adds or deletes one or more elements, or explicitly
 * resizes the backing array; merely setting the value of an element is not
 * a structural modification.)  This is typically accomplished by
 * synchronizing on some object that naturally encapsulates the list.
 *
 * If no such object exists, the list should be "wrapped" using the
 * {@link Collections#synchronizedList Collections.synchronizedList}
 * method.  This is best done at creation time, to prevent accidental
 * unsynchronized access to the list:<pre>
 *   List list = Collections.synchronizedList(new ArrayList(...));</pre>
 *
 * <p>The iterators returned by this class's <tt>iterator</tt> and
 * <tt>listIterator</tt> methods are <i>fail-fast</i>: if the list is
 * structurally modified at any time after the iterator is created, in any way
 * except through the iterator's own <tt>remove</tt> or <tt>add</tt> methods,
 * the iterator will throw a {@link ConcurrentModificationException}.  Thus, in
 * the face of concurrent modification, the iterator fails quickly and cleanly,
 * rather than risking arbitrary, non-deterministic behavior at an undetermined
 * time in the future.<p>
 *
 * Note that the fail-fast behavior of an iterator cannot be guaranteed
 * as it is, generally speaking, impossible to make any hard guarantees in the
 * presence of unsynchronized concurrent modification.  Fail-fast iterators
 * throw <tt>ConcurrentModificationException</tt> on a best-effort basis.
 * Therefore, it would be wrong to write a program that depended on this
 * exception for its correctness: <i>the fail-fast behavior of iterators
 * should be used only to detect bugs.</i><p>
 *
 * This class is a member of the
 * <a href="{@docRoot}/../technotes/guides/collections/index.html">
 * Java Collections Framework</a>.
 *
 * @author  Josh Bloch
 * @author  Neal Gafter
 * @version %I%, %G%
 * @see	    Collection
 * @see	    List
 * @see	    LinkedList
 * @see	    Vector
 * @since   1.2
 */

public class BucketArrayList<E> 
implements RandomAccess, java.io.Serializable, Iterable<E>
{
	private static final long serialVersionUID = 8683452581122892189L;

	/**
	 * The array buffer into which the elements of the ArrayList are stored.
	 * The capacity of the ArrayList is the length of this array buffer.
	 */
	private transient Object[] bucket;

	/**
	 * The size of the ArrayList (the number of elements it contains).
	 *
	 * @serial
	 */
	private int size;
	
	private final int bucketExp = 10; 
	private final int bucketSize = 1 << 10; //1024
	private int bucketDepth;

	private int modCount = 0;
	
	/**
	 * Constructs an empty list with an initial capacity of ten.
	 */
	public BucketArrayList() {
		bucket = new Object[bucketSize];
	}

	/**
	 * Trims the capacity of this <tt>ArrayList</tt> instance to be the
	 * list's current size.  An application can use this operation to minimize
	 * the storage of an <tt>ArrayList</tt> instance.
	 */
	public void trimToSize() {
		modCount++;
		while ((bucketDepth > 0) && (bucketSize << ((bucketDepth-1)*bucketExp) >= size)) {
			bucket = (Object[]) bucket[0];
		}
	}

	/**
	 * Increases the capacity of this <tt>ArrayList</tt> instance, if
	 * necessary, to ensure that it can hold at least the number of elements
	 * specified by the minimum capacity argument.
	 *
	 * @param   minCapacity   the desired minimum capacity
	 */
	public void ensureCapacity(int minCapacity) {
		modCount++;
		while (bucketSize << (bucketDepth*bucketExp) < minCapacity) {
			Object oldData[] = bucket;
			bucket = new Object[bucketSize];
			bucket[0] = oldData;
			bucketDepth++;
		}
	}

	/**
	 * Returns the number of elements in this list.
	 *
	 * @return the number of elements in this list
	 */
	public int size() {
		return size;
	}

	/**
	 * Returns <tt>true</tt> if this list contains no elements.
	 *
	 * @return <tt>true</tt> if this list contains no elements
	 */
	public boolean isEmpty() {
		return size == 0;
	}

	// Positional Access Operations

	/**
	 * Returns the element at the specified position in this list.
	 *
	 * @param  index index of the element to return
	 * @return the element at the specified position in this list
	 * @throws IndexOutOfBoundsException {@inheritDoc}
	 */
	public E get(int index) {
		RangeCheck(index);

		return getElement(bucket, bucketDepth, index);
	}

	private E getElement(Object[] parent, int depth, int index) {
		if (depth > 0) {
			int pos = index >> (depth*bucketExp);
			int index2 = ((1 << (depth*bucketExp))-1) & index;
			return getElement((Object[]) parent[pos], depth-1, index2);
		}
		return (E) parent[index];
	}
	
	/**
	 * Replaces the element at the specified position in this list with
	 * the specified element.
	 *
	 * @param index index of the element to replace
	 * @param element element to be stored at the specified position
	 * @return the element previously at the specified position
	 * @throws IndexOutOfBoundsException {@inheritDoc}
	 */
	public E set(int index, E e) {
		RangeCheck(index);

		return addElement(bucket, bucketDepth, index, e);
	}

	/**
	 * Appends the specified element to the end of this list.
	 *
	 * @param e element to be appended to this list
	 * @return <tt>true</tt> (as specified by {@link Collection#add})
	 */
	public boolean add(E e) {
		ensureCapacity(size + 1);  // Increments modCount!!
		addElement(bucket, bucketDepth, size++, e);
		return true;
	}

	private E addElement(Object[] parent, int depth, int index, E e) {
		if (depth > 0) {
			int pos = index >> (depth*bucketExp);
			int index2 = ((1 << (depth*bucketExp))-1) & index;
			if (parent[pos] == null) {
				parent[pos] = new Object[bucketSize];
			}
			return addElement((Object[]) parent[pos], depth-1, index2, e);
		}
		E old = (E) parent[index];
		parent[index] = e;
		return old;
	}
	
	/**
	 * Inserts the specified element at the specified position in this
	 * list. Shifts the element currently at that position (if any) and
	 * any subsequent elements to the right (adds one to their indices).
	 *
	 * @param index index at which the specified element is to be inserted
	 * @param element element to be inserted
	 * @throws IndexOutOfBoundsException {@inheritDoc}
	 */
//	public void add(int index, E element) {
//		if (index > size || index < 0)
//			throw new IndexOutOfBoundsException(
//					"Index: "+index+", Size: "+size);
//
//		ensureCapacity(size+1);  // Increments modCount!!
//		System.arraycopy(elementData, index, elementData, index + 1,
//				size - index);
//		elementData[index] = element;
//		size++;
//	}

	/**
	 * Removes the element at the specified position in this list.
	 * Shifts any subsequent elements to the left (subtracts one from their
	 * indices).
	 *
	 * @param index the index of the element to be removed
	 * @return the element that was removed from the list
	 * @throws IndexOutOfBoundsException {@inheritDoc}
	 */
	public E remove(int index) {
		RangeCheck(index);

		modCount++;
		
		if (index == size - 1) {
			//TODO clean up!
			E obj = get(index);
			size--;
			return obj;
		}
		throw new UnsupportedOperationException();
	}

	/**
	 * Removes all of the elements from this list.  The list will
	 * be empty after this call returns.
	 */
	public void clear() {
		modCount++;

		// this does not allocate more memory:
		for (int i = 0; i < bucketSize; i++) {
			bucket[i] = null;
		}

		bucketDepth = 0;
		size = 0;
	}

	/**
	 * Checks if the given index is in range.  If not, throws an appropriate
	 * runtime exception.  This method does *not* check if the index is
	 * negative: It is always used immediately prior to an array access,
	 * which throws an ArrayIndexOutOfBoundsException if index is negative.
	 */
	private void RangeCheck(int index) {
		if (index >= size) {
			throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
		}
	}

	@Override
	public Iterator<E> iterator() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	public String[] toArray(String[] strings) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	public Object[] toArray() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	public void addAll(int i, LinkedList<String> l) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//
	}

	public void addAll(LinkedList<String> l) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//
	}

	public void remove(String element2) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//
	}
}

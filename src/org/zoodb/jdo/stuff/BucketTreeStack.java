package org.zoodb.jdo.stuff;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.RandomAccess;

/**
 * Bucket array list with fast random access by index. It has concurrency potential.
 * 
 * Works like an ArrayList but is much faster when getting big, because the bucket architecture
 * avoids copying the existing list to a new array as happens in the ArrayList.
 * 
 * Features:
 * - Fast insert, traversal and access by index.
 * - Some potential as CONCURRENT collection, because an existing tree is only changed by adding 
 *   elements to the end or by inserting a higher order root. But an existing section of the tree
 *   never changes (except clean() and remove(), see remove()), so iterators are inherently stable.
 *  
 * Limitations:
 * - Only the last element can be removed!
 * - Elements can only be added to the end.
 * 
 * @author Tilmann Zäschke
 */

public class BucketTreeStack<E> 
implements RandomAccess, java.io.Serializable, Iterable<E>
{
	private static final long serialVersionUID = 8683452581122892189L;

	private transient Object[] bucket;

	/**
	 * The size of the BucketList (the number of elements it contains).
	 */
	private int size;
	
	private final int bucketExp; 
	private final int bucketSize;
	private int bucketDepth;

	private int modCount = 0;
	
	/**
	 * Constructs an empty list with an initial capacity of ten.
	 */
	public BucketTreeStack() {
		this((byte) 10); //1024
	}

	/**
	 * Constructs an empty list with an initial capacity of ten.
	 * @param bucketExponent List buckets will contain 2^bucketExponent elements.
	 */
	public BucketTreeStack(byte bucketExponent) {
		this.bucketExp = bucketExponent;
		this.bucketSize = 1 << bucketExponent;
		bucket = new Object[bucketSize];
	}

	/**
	 * Trims the capacity of this <tt>BucketArrayList</tt> instance to be the
	 * list's current size.  An application can use this operation to minimize
	 * the storage of an <tt>BucketArrayList</tt> instance.
	 */
	public void trimToSize() {
		modCount++;
		while ((bucketDepth > 0) && (bucketSize << ((bucketDepth-1)*bucketExp) >= size)) {
			bucket = (Object[]) bucket[0];
		}
	}

	/**
	 * Increases the capacity of this <tt>BucketArrayList</tt> instance, if
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
		rangeCheck(index);

		return getElement(bucket, bucketDepth, index);
	}

	@SuppressWarnings("unchecked")
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
		rangeCheck(index);

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

	@SuppressWarnings("unchecked")
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
	 * Removes the element at the specified position in this list.
	 * Only the last element in the list can be removed.
	 *
	 * @param index the index of the element to be removed
	 * @return the element that was removed from the list
	 * @throws IndexOutOfBoundsException {@inheritDoc}
	 */
	public E removeLast() {
		modCount++;
		
		//TODO clean up reference?
		//If we don't clean it up, we get a (small?) memory leak.
		//If we clean it up, the iterator may not be stable anymore. 
		E obj = get(size - 1);
		size--;
		return obj;

//		rangeCheck(index);
//
//		modCount++;
//		
//		if (index == size - 1) {
//			E obj = get(index);
//			size--;
//			return obj;
//		}
//		throw new UnsupportedOperationException();
	}

	/**
	 * Removes all of the elements from this list.  The list will
	 * be empty after this call returns.
	 */
	public void clear() {
		modCount++;

		// this does not allocate more memory:
		//TODO possibly faster: create new empty bucket? -> test speeds create vs set-to-null
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
	 * which throws an IndexOutOfBoundsException if index is negative.
	 */
	private void rangeCheck(int index) {
		if (index >= size) {
			throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
		}
	}

	@Override
	public Iterator<E> iterator() {
		return new BucketIterator(size - 1);
	}
	
	/**
	 * Iterator for BucketArrayList.
	 * Concurrency: 
	 * - If clear() and remove() are not used, concurrency is simple, we just remember the current 
	 *   max and iterator to it. This is effectively a COW iterator. If clear() and remove() are
	 *   used, we would have to make sure that the references are not deleted. For example:
	 *   remove() keeps references; only clear() removes the whole tree (replace by new empty 
	 *   bucket); but the iterator could keep a reference to the tree as long as required.
	 * - We could make the iterator adaptive, so it only iterates to the end of the current list.
	 *   This works nicely with clear() and remove(), but results in a somewhat unusual concurrency
	 *   policy.
	 * TODO choose and implement.
	 * 
	 *  TODO
	 *  Other improvement: may it be faster to store a stack here and keep a reference to the 
	 *  current bucket? Iteration should be faster for large pages, but Iterator creation is slower.
	 *  But for large pages, using get to find them should be fast as well. 
	 */
	private class BucketIterator implements Iterator<E> {
		
		private int pos = 0;
		private final int max;
		
		/**
		 * @param max Maximum index = (size-1)
		 */
		private BucketIterator(int max) {
			this.max = max;
		}

		@Override
		public boolean hasNext() {
			return pos <= max;
		}

		@Override
		public E next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			return get(pos++);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}		
	}
}

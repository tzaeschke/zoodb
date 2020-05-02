/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zoodb.internal.util;

import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.RandomAccess;

/**
 * Bucket array list without random access. This is best used as stack.
 * 
 * See PerfIterator for performance considerations.
 * 
 * Works like an ArrayList but is much faster when getting big, because the bucket architecture
 * avoids copying the existing list to a new array as happens in the ArrayList.
 * 
 * Features:
 * - Fast adding, removing and traversal.
 * - Speed of all operations (except clear()) independent of size.
 *  
 * Limitations:
 * - Only the last element can be removed!
 * - Elements can only be added to the end.
 * 
 * @author Tilmann Zaeschke
 */
public class BucketStack<E> 
implements RandomAccess, java.io.Serializable, Iterable<E>
{
	private static final long serialVersionUID = 8683452581122892189L;

	//TODO use bucket List!
	private final LinkedList<E[]> buckets = new LinkedList<E[]>();

	/**
	 * The size of the BucketList (the number of elements it contains).
	 */
	private int size = 0;
	private int cntInBucket;
	
	private final int bucketSize;

	//TODO this is not used much...
	private int modCount = 0;
	
	/**
	 * Constructs an empty list with an initial capacity of ten.
	 */
	public BucketStack() {
		this(1000);
	}

	/**
	 * Constructs an empty list with an initial capacity of ten.
	 * @param bucketSize The bucket size
	 */
	public BucketStack(int bucketSize) {
		this.bucketSize = bucketSize;
		cntInBucket = bucketSize;  //special value for empty list
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


	/**
	 * Appends the specified element to the end of this list.
	 *
	 * @param e element to be appended to this list
	 * @return <tt>true</tt> (as specified by {@link Collection#add})
	 */
	@SuppressWarnings("unchecked")
	public boolean push(E e) {
		if (cntInBucket >= bucketSize) {
			buckets.add((E[]) new Object[bucketSize]);
			cntInBucket = 0;
		}
		buckets.getLast()[cntInBucket++] = e;
		size++;
		return true;
	}
	
	public E peek() {
		return buckets.getLast()[cntInBucket-1];
	}

	/**
	 * Removes the element at the specified position in this list.
	 * Only the last element in the list can be removed.
	 *
	 * @return the element that was removed from the list
	 * @throws NoSuchElementException if no elements remain
	 */
	public E pop() {
		modCount++;
		
		if (size == 0) {
			throw new NoSuchElementException();
		}
		
		size--;

		if (cntInBucket == 1) {
			cntInBucket = bucketSize;
			return buckets.removeLast()[0];
		}
		return buckets.getLast()[--cntInBucket];
	}

	/**
	 * Removes all of the elements from this list.  The list will
	 * be empty after this call returns.
	 */
	public void clear() {
		modCount++;

		buckets.clear();
		cntInBucket = bucketSize;
		size = 0;
	}

	@Override
	public Iterator<E> iterator() {
		return new BucketIterator(buckets.iterator(), size % bucketSize, modCount);
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
		
		private int pos = bucketSize;
		private E[] currentBucket;
		private final Iterator<E[]> buckets;
		private final int cntLast;
		private final int modCountI;
		
		private BucketIterator(Iterator<E[]> buckets, int cntLast, int modCountI) {
			this.buckets = buckets;
			this.cntLast = cntLast == 0 ? bucketSize : cntLast;
			this.modCountI = modCountI;
		}

		@Override
		public boolean hasNext() {
			if (buckets.hasNext()) {
				return true;
			}
			return pos < cntLast;
		}

		@Override
		public E next() {
			if (modCountI != modCount) {
				throw new ConcurrentModificationException();
			}
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			
			if (pos >= bucketSize) {
				pos = 0;
				currentBucket = buckets.next();
			}
			return currentBucket[pos++];
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}		
	}
}

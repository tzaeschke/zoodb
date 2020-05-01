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

import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * 
 * @author Tilmann Zaeschke
 */
public class PrimLongArrayList implements Iterable<Long> {

	private static final long[] EMPTY_LIST = {};
	private static final int MIN_SIZE_INCREMENT = 10;

	private int modCount = 0;
	private int size;
	private long[] list = EMPTY_LIST;

	public PrimLongArrayList() {
		//
	}

	/**
	 * @param val The value that should be checked
	 * @return <tt>true</tt> if the list {@code val}
	 */
	public boolean contains(long val) {
		for (int i = 0; i < size; i++) {
			if (list[i] == val) {
				return true;
			}
		}
		return false;
	}

	public long get(int pos) {
		checkPos(pos);
		return list[pos];
	}

	private void checkPos(int pos) {
		if (pos < 0 || pos >= size) {
			throw new IndexOutOfBoundsException("pos=" + pos);
		}
	}

	public void set(int pos, long value) {
		checkPos(pos);
		list[pos] = value;
	}

	public void add(long value) {
		ensureCapacity(size + 1);
		list[size] = value;
		size++;
	}

	public long removePos(int pos) {
		checkPos(pos);
		long prevVal = list[pos];
		removeNoCheck(pos);
		return prevVal;
	}

	public boolean removeVal(long value) {
		for (int i = 0; i < size; i++) {
			if (list[i] == value) {
				removeNoCheck(i);
				return true;
			}
		}
		return false;
	}

	public long removeFirst() {
		return removePos(0);
	}

	public long removeLast() {
		return removePos(size - 1);
	}

	private void removeNoCheck(int pos) {
		modCount++;
		size--;
		int len = size - pos;
		//TODO allow shrinking here?
		if (len > 0) {
			System.arraycopy(list, pos+1, list, pos, len);
		}
	}

	public void addAll(Collection<Long> c) {
		ensureCapacity(size + c.size());
		for (Long l: c) {
			list[size] = l;
			size++;
		}
	}

	public void addAll(PrimLongArrayList list2) {
		ensureCapacity(size + list2.size);
		System.arraycopy(list2.list, 0, list, size, list2.size);
		size += list2.size;
	}

	private void ensureCapacity(int required) {
		int toAdd = required - list.length;
		if (toAdd <= 0) {
			return;
		}

		int minIncrement = MIN_SIZE_INCREMENT < (size>>>1) ? (size >>>1) : MIN_SIZE_INCREMENT;
		int newSize = size + ((toAdd < minIncrement) ? minIncrement : toAdd);

		long[] newList = new long[newSize];
		System.arraycopy(list, 0, newList, 0, size);
		list = newList;
	}

	/**
	 * @return the size of the list
	 */
	public int size() {
		return size;
	}

	/**
	 * @return {@code true} if the list is empty, othewise {@code false}
	 */
	public boolean isEmpty() {
		return size == 0;
	}

	public void clear() {
		modCount++;
		list = EMPTY_LIST;
		size = 0;
	}

	public long[] toArray() {
		return Arrays.copyOf(list, size);
	}

	@Override
	public LongIterator iterator() {
		return new LongIterator();
	}

	public class LongIterator implements Iterator<Long> {
		private int expectedModCount = modCount;
		private int posNextToReturn = 0;

		@Override
		public boolean hasNext() {
			return hasNextLong();
		}

		@Override
		public Long next() {
			return nextLong();
		}

		public boolean hasNextLong() {
			return posNextToReturn < size;
		}

		public long nextLong() {
			checkModCount();
			if (!hasNextLong()) {
				throw new NoSuchElementException();
			}
			return list[posNextToReturn++];
		}

		@Override
		public void remove() {
			checkModCount();
			if (posNextToReturn <= 0) {
				throw new IllegalStateException();
			}
			PrimLongArrayList.this.removeNoCheck(posNextToReturn - 1);
			expectedModCount = modCount;
		}

		private void checkModCount() {
			if (modCount != expectedModCount) {
				throw new ConcurrentModificationException();
			}
		}
	}

}

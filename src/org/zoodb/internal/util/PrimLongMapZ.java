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
import java.util.NoSuchElementException;
import java.util.Set;

public class PrimLongMapZ<T> implements PrimLongMap<T> {

	public static class Entry<T> implements PrimLongEntry<T> {
		final long key;
		T value;
		Entry<T> next = null;
		Entry(long key, T value) {
			this.key = key;
			this.value = value;
		}
		@Override
		public long getKey() {
			return key;
		}
		@Override
		public T getValue() {
			return value;
		}
		public void setValue(T value) {
			this.value = value;
		}
	}
	
	private static final double LOAD_FACTOR = 0.75;
	
	private Entry<T>[] entries;
	
	private int capacityPower = 6;
	//capacity = 2^capacityPower
	private int limitMax = (int) ((1 << capacityPower) * LOAD_FACTOR);
	private int size = 0;
	
	private int modCount = 0;

	private PrimLongValues valueResult;
	private EntrySet entryResult;
	private KeySet keyResult;
	
	public PrimLongMapZ() {
		this(60);
	}
	
	@SuppressWarnings("unchecked")
	public PrimLongMapZ(int capacity) {
		capacityPower = 1;
		while (1 << capacityPower < capacity) {
			capacityPower++;
		}
		int capacity2 = 1 << capacityPower;
		limitMax = (int) (capacity2 * LOAD_FACTOR);
		entries = new Entry[capacity2];
	}
	
	
	private int calcHash(long key) {
		//This works well with primes:
		//return (int) key % capacity;
		//Knuths multiplicative hash function
		return (int)(key * 2654435761L) >>> (32 - capacityPower);
	}
	
	@SuppressWarnings("unchecked")
	private void checkRehash(int newSize) {
		if (newSize > limitMax && capacityPower < 31) {
			capacityPower++;
			int capacity = 1 << capacityPower;
			limitMax = (int) (capacity * LOAD_FACTOR);
			Entry<T>[] oldEntries = entries;
			entries = new Entry[capacity];
			for (int i = 0; i < oldEntries.length; i++) {
				Entry<T> e = oldEntries[i];
				while (e != null) {
					Entry<T> eNext = e.next;
					e.next = null;
					putEntryNoCheck(e);
					e = eNext;
				}
			}
		}
	}
	
	private void putEntryNoCheck(Entry<T> e) {
		int pos = calcHash(e.key);
		e.next = entries[pos];
		entries[pos] = e;  
	}
	
	@Override
	public T get(long keyBits) {
		int pos = calcHash(keyBits);
		Entry<T> e = entries[pos];
		while (e != null && e.key != keyBits) {
			e = e.next;
		}
		return e == null ? null : e.getValue();
	}

	@Override
	public T put(long keyBits, T obj) {
		if (obj == null) {
			throw new IllegalArgumentException("Value must not be null.");
		}
		int pos = calcHash(keyBits);
		Entry<T> e = entries[pos];
		while (e != null && e.key != keyBits) {
			e = e.next;
		}
		modCount++;
		if (e != null) {
			T ret = e.getValue();
			e.setValue(obj);
			return ret;
		}
		checkRehash(size + 1);
		putEntryNoCheck(new Entry<T>(keyBits, obj));
		size++;
		return null;
	}

	@Override
	public T putIfAbsent(long keyBits, T obj) {
		if (obj == null) {
			throw new IllegalArgumentException("Value must not be null.");
		}
		int pos = calcHash(keyBits);
		Entry<T> e = entries[pos];
		while (e != null && e.key != keyBits) {
			e = e.next;
		}
		if (e != null) {
			return e.getValue();
		}
		modCount++;
		checkRehash(size + 1);
		putEntryNoCheck(new Entry<T>(keyBits, obj));
		size++;
		return null;
	}

	@Override
	public T remove(long keyBits) {
		int pos = calcHash(keyBits);
		Entry<T> e = entries[pos];
		Entry<T> prev = null;
		while (e != null && e.key != keyBits) {
			prev = e;
			e = e.next;
		}
		if (e != null) {
			//remove
			modCount++;
			size--;
			T ret = e.getValue();
			if (prev != null) {
				prev.next = e.next;
			} else {
				entries[pos] = e.next;
			}
			return ret;
		}
		return null;
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public PrimLongValues values() {
		if (valueResult == null) {
			valueResult = new PrimLongValues();
		}
		return valueResult;
	}

	@Override
	public void clear() {
		for (int i = 0; i < entries.length; i++) {
			entries[i] = null;
		}
		size = 0;
		modCount++;
	}

	@Override
	public boolean containsKey(long keyBits) {
		int pos = calcHash(keyBits);
		Entry<T> e = entries[pos];
		while (e != null && e.key != keyBits) {
			e = e.next;
		}
		return e != null;
	}

	@Override
	public boolean containsValue(T value) {
		for (int i = 0; i < entries.length; i++) {
			Entry<T> e = entries[i];
			while (e != null) {
				if (e.getValue() == value) {
					return true;
				}
				e = e.next;
			}
		}
		return false;
	}

	@Override
	public void putAll(PrimLongMap<? extends T> map) {
		for (PrimLongEntry<? extends T> e : map.entrySet()) {
			put(e.getKey(), e.getValue());
		}
	}

	@Override
	public Set<Long> keySet() {
		if (keyResult == null) {
			keyResult = new KeySet();
		}
		return keyResult;
	}

	@Override
	public Set<PrimLongEntry<T>> entrySet() {
		if (entryResult == null) {
			entryResult = new EntrySet();
		}
		return entryResult;
	}

	private class EntrySet extends ResultSet<PrimLongEntry<T>> {

		@Override
		public Iterator<PrimLongEntry<T>> iterator() {
			return new EntryIterator();
		}
	}
	
	class EntryIterator implements Iterator<PrimLongEntry<T>> {
		private int pos = -1;
		private Entry<T> next;
		private final int currentModCount;
		public EntryIterator() {
			currentModCount = modCount;
			while (++pos < entries.length) {
				if (entries[pos] != null) {
					next = entries[pos];
					break;
				}
			}
		}
		
		private void findNext() {
			if (next.next != null) {
				next = next.next;
				return;
			}
			
			while (++pos < entries.length) {
				if (entries[pos] != null) {
					next = entries[pos];
					return;
				}
			}
			next = null;
		}
		
		@Override
		public boolean hasNext() {
			return next != null;
		}

		@Override
		public PrimLongEntry<T> next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			if (currentModCount != modCount) {
				throw new ConcurrentModificationException();
			}
			Entry<T> t = next;
			findNext();
			return t;
		}
	}


	private class KeySet extends ResultSet<Long> {

		@Override
		public Iterator<Long> iterator() {
			return new KeyIterator();
		}
	}
	
	class KeyIterator implements Iterator<Long> {
		private int pos = -1;
		private Entry<T> next;
		private final int currentModCount;
		public KeyIterator() {
			currentModCount = modCount;
			while (++pos < entries.length) {
				if (entries[pos] != null) {
					next = entries[pos];
					break;
				}
			}
		}
		
		private void findNext() {
			if (next.next != null) {
				next = next.next;
				return;
			}
			
			while (++pos < entries.length) {
				if (entries[pos] != null) {
					next = entries[pos];
					return;
				}
			}
			next = null;
		}
		
		@Override
		public boolean hasNext() {
			return next != null;
		}

		@Override
		public Long next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			if (currentModCount != modCount) {
				throw new ConcurrentModificationException();
			}
			Entry<T> t = next;
			findNext();
			return t.key;
		}
	}


	public class PrimLongValues extends ResultSet<T> {

		@Override
		public Iterator<T> iterator() {
			return new ValuesIterator();
		}
	}
	
	class ValuesIterator implements Iterator<T> {
		private int pos = -1;
		private Entry<T> next;
		private int currentModCount;
		private Entry<T> prevEntry = null;
		public ValuesIterator() {
			this.currentModCount = modCount;
			findNext();
		}
		
		private void findNext() {
			if (next != null && next.next != null) {
				next = next.next;
				return;
			}
			
			while (++pos < entries.length) {
				if (entries[pos] != null) {
					next = entries[pos];
					return;
				}
			}
			next = null;
		}
		
		@Override
		public boolean hasNext() {
			return next != null;
		}

		@Override
		public T next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			if (currentModCount != modCount) {
				throw new ConcurrentModificationException();
			}
			Entry<T> t = next;
			prevEntry = t;
			findNext();
			return t.getValue();
		}
		
		@Override
		public void remove() {
			if (currentModCount != modCount) {
				throw new ConcurrentModificationException();
			}
			if (prevEntry == null) {
				//we need to call next() first...
				throw new NoSuchElementException();
			}
			//TODO this could be faster if we remember the previous position...
			PrimLongMapZ.this.remove(prevEntry.key);
			currentModCount = modCount;
		}
	}


	private abstract class ResultSet<R> implements Set<R> {
		@Override
		public int size() {
			return PrimLongMapZ.this.size();
		}

		@Override
		public boolean isEmpty() {
			return size() == 0;
		}

		@Override
		public boolean contains(Object o) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Object[] toArray() {
			throw new UnsupportedOperationException();
		}

		@Override
		public <R2> R2[] toArray(R2[] a) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean add(R e) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean remove(Object o) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean containsAll(Collection<?> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean addAll(Collection<? extends R> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean retainAll(Collection<?> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean removeAll(Collection<?> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException();
		}
		
	}


	public boolean isEmpty() {
		return size == 0;
	}
	
}

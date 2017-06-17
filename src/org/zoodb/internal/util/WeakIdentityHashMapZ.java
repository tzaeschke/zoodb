/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
 * 
 * This file is part of ZooDB.
 * 
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * See the README and COPYING files for further information. 
 */
package org.zoodb.internal.util;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public class WeakIdentityHashMapZ<K, T> implements Map<K, T> {

	public static class Entry<K, T> extends WeakReference<K> implements Map.Entry<K, T> {
		int hashCode;
		T value;
		Entry<K, T> nextE = null;
		@SuppressWarnings("unchecked")
		Entry(int hashCode, K key, T value, ReferenceQueue<K> queue) {
			super(key == null ? (K)NULL : key, queue);
			this.hashCode = hashCode;
			this.value = value;
		}

		public K getKeyNotNull() {
			return super.get();
		}

		@Override
		public K get() {
			throw new UnsupportedOperationException();
		}

		@Override
		public T getValue() {
			return value;
		}
		
		@Override
		public K getKey() {
			return super.get() == NULL ? null : super.get();
		}

		@Override
		public T setValue(T value) {
			T ret = this.value;
			this.value = value;
			return ret;
		}
	}
	
	private static final double LOAD_FACTOR = 0.75;
	private static final Object NULL = new Object();
	private final ReferenceQueue<K> queue = new ReferenceQueue<>();
	
	private Entry<K, T>[] entries;
	
	private int capacityPower = 6;
	//capacity = 2^capacityPower
	private int limitMax = (int) ((1 << capacityPower) * LOAD_FACTOR);
	private int size = 0;
	
	private int modCount = 0;

	private Values valueResult;
	private EntrySet entryResult;
	private KeySet keyResult;
	
	public WeakIdentityHashMapZ() {
		this(60);
	}
	
	@SuppressWarnings("unchecked")
	public WeakIdentityHashMapZ(int capacity) {
		capacityPower = 1;
		while (1 << capacityPower < capacity) {
			capacityPower++;
		}
		int capacity2 = 1 << capacityPower;
		limitMax = (int) (capacity2 * LOAD_FACTOR);
		entries = new Entry[capacity2];
	}
	
	
	private int calcHash(Object key) {
		//This works well with primes:
		//return (int) key % capacity;
		//Knuths multiplicative hash function
		return (int)(System.identityHashCode(key) * 2654435761L) >>> (32 - capacityPower);
	}
	
	@SuppressWarnings("unchecked")
	private void checkRehash(int newSize) {
		if (newSize > limitMax && capacityPower < 31) {
			capacityPower++;
			int capacity = 1 << capacityPower;
			limitMax = (int) (capacity * LOAD_FACTOR);
			Entry<K, T>[] oldEntries = entries;
			entries = new Entry[capacity];
			for (int i = 0; i < oldEntries.length; i++) {
				Entry<K, T> e = oldEntries[i];
				while (e != null) {
					Entry<K, T> eNext = e.nextE;
					e.nextE = null;
					putEntryNoCheck(e);
					e = eNext;
				}
			}
		}
	}
	
//	private void printHisto() {
//		int[] histo = new int[size];
//		for (int i = 0; i < entries.length; i++) {
//			int n = 0;
//			Entry<T> e = entries[i];
//			while (e != null) {
//				n++;
//				e = e.nextE;
//			}
//			histo[n]++;
//		}
//		System.out.println("Histo: " + Arrays.toString(histo));
//	}
	
	private void putEntryNoCheck(Entry<K, T> e) {
		Object key = e.getKeyNotNull();
		if (key == null) {
			size--;
			return;
		}
		int pos = calcHash(key);
		e.hashCode = pos;
		e.nextE = entries[pos];
		entries[pos] = e;  
	}
	
	@Override
	public T get(Object key) {
		cleanup();
		int pos = calcHash(key);
		Entry<K, T> e = entries[pos];
		Object keyOrNULL = mapToNULL(key);
		while (e != null && e.getKeyNotNull() != keyOrNULL) {
			e = e.nextE;
		}
		//We do not clean up gc'd Entries here, this may cause ConcurrentModificationException
		return e == null ? null : e.getValue();
	}

	@Override
	public T put(K key, T obj) {
		cleanup();
		int pos = calcHash(key);
		
		Entry<K, T> e = entries[pos];
		Object keyOrNULL = mapToNULL(key);
		while (e != null && e.getKeyNotNull() != keyOrNULL) {
			e = e.nextE;
		}
		modCount++;
		if (e != null) {
			//this works fine with weak refs
			T ret = e.getValue();
			e.setValue(obj);
			return ret;
		}
		checkRehash(size + 1);
		putEntryNoCheck(new Entry<K, T>(pos, key, obj, queue));
		size++;
		return null;
	}

	@Override
	public T remove(Object key) {
		cleanup();
		int pos = calcHash(key);
		Entry<K, T> e = entries[pos];
		Entry<K, T> prev = null;
		Object keyOrNULL = mapToNULL(key);
		while (e != null && e.getKeyNotNull() != keyOrNULL) {
			prev = e;
			e = e.nextE;
		}
		if (e != null) {
			//remove
			modCount++;
			size--;
			//this works fine with weak refs
			T ret = e.getValue();
			if (prev != null) {
				prev.nextE = e.nextE;
			} else {
				entries[pos] = e.nextE;
			}
			return ret;
		}
		return null;
	}

	private void remove(Entry<K, T> eDelete) {
		int pos = eDelete.hashCode;
		Entry<K, T> e = entries[pos];
		Entry<K, T> prev = null;
		while (e != null && e != eDelete) {
			prev = e;
			e = e.nextE;
		}
		if (e != null) {
			//remove
			modCount++;
			size--;
			//this works fine with weak refs
			if (prev != null) {
				prev.nextE = e.nextE;
			} else {
				entries[pos] = e.nextE;
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void cleanup() {
		Entry<K, T> e;
		while ((e = (Entry<K, T>) queue.poll()) != null) {
			remove(e);
		}
	}
	
	@Override
	public int size() {
		if (size > 0) {
			cleanup();
		}
		return size;
	}

	@Override
	public Collection<T> values() {
		if (valueResult == null) {
			valueResult = new Values();
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
		//clear up. No removal necessary.
		while (queue.poll() != null) {
			//nothing
		}
	}

	@Override
	public boolean containsKey(Object key) {
		cleanup();
		int pos = calcHash(key);
		Entry<K, T> e = entries[pos];
		Object keyOrNULL = mapToNULL(key);
		while (e != null && e.getKeyNotNull() != keyOrNULL) {
			e = e.nextE;
		}
		return e != null;
	}

	private static final Object mapToNULL(Object o) {
		return o == null ? NULL : o;
	}


	private final K unmapKey(K nextK) {
		return nextK == NULL ? null : nextK;
	}

	@Override
	public boolean containsValue(Object value) {
		for (int i = 0; i < entries.length; i++) {
			Entry<K, T> e = entries[i];
			while (e != null) {
				//This works fine with weak refs.
				//No cleaning up here to avoid concurrent modification.
				if (e.getValue() == value) {
					return true;
				}
				e = e.nextE;
			}
		}
		return false;
	}

	@Override
	public void putAll(Map<? extends K, ? extends T> map) {
		for (Map.Entry<? extends K, ? extends T> e : map.entrySet()) {
			put(e.getKey(), e.getValue());
		}
	}

	@Override
	public Set<K> keySet() {
		if (keyResult == null) {
			keyResult = new KeySet();
		}
		return keyResult;
	}

	@Override
	public Set<Map.Entry<K, T>> entrySet() {
		if (entryResult == null) {
			entryResult = new EntrySet();
		}
		return entryResult;
	}

	private class EntrySet extends ResultSet<Map.Entry<K, T>> {

		@Override
		public Iterator<Map.Entry<K, T>> iterator() {
			return new EntryIterator();
		}
	}
	
	class EntryIterator implements Iterator<Map.Entry<K, T>> {
		private int pos = -1;
		private Entry<K, T> next;
		//this dummy represents an Entry whose 'next' is the first of a slot
		private final Entry<K, T> newSlotDummy = new Entry<K, T>(0, null, null, queue);
		@SuppressWarnings("unused")
		private K nextK;
		private int currentModCount;
		public EntryIterator() {
			currentModCount = modCount;
			findNext();
		}
		
		private void findNext() {
			findNextUnsafe();
			while (next != null && (nextK = next.getKeyNotNull()) == null) {
				//remove
				newSlotDummy.nextE = next.nextE;
				WeakIdentityHashMapZ.this.remove(next);
				next = newSlotDummy;
				currentModCount = modCount;
				findNextUnsafe();
			}
		}
		
		private void findNextUnsafe() {
			if (next != null && next.nextE != null) {
				next = next.nextE;
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
		public Entry<K, T> next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			if (currentModCount != modCount) {
				throw new ConcurrentModificationException();
			}
			Entry<K, T> t = next;
			findNext();
			return t;
		}
	}


	private class KeySet extends ResultSet<K> {

		@Override
		public Iterator<K> iterator() {
			return new KeyIterator();
		}
	}
	
	class KeyIterator implements Iterator<K> {
		private int pos = -1;
		private Entry<K, T> next;
		//this dummy represents an Entry whose 'next' is the first of a slot
		private final Entry<K, T> newSlotDummy = new Entry<K, T>(0, null, null, queue);
		private K nextK;
		private int currentModCount;
		public KeyIterator() {
			currentModCount = modCount;
			findNext();
		}
		
		private void findNext() {
			findNextUnsafe();
			while (next != null && (nextK = next.getKeyNotNull()) == null) {
				//remove
				newSlotDummy.nextE = next.nextE;
				WeakIdentityHashMapZ.this.remove(next);
				next = newSlotDummy;
				currentModCount = modCount;
				findNextUnsafe();
			}
		}
		
		private void findNextUnsafe() {
			if (next != null && next.nextE != null) {
				next = next.nextE;
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
		public K next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			if (currentModCount != modCount) {
				throw new ConcurrentModificationException();
			}
			K key = nextK;
			findNext();
			return unmapKey(key);
		}
	}


	private class Values extends ResultSet<T> {

		@Override
		public Iterator<T> iterator() {
			return new ValuesIterator();
		}
	}
	
	class ValuesIterator implements Iterator<T> {
		private int pos = -1;
		private Entry<K, T> next;
		//this dummy represents an Entry whose 'next' is the first of a slot
		private final Entry<K, T> newSlotDummy = new Entry<K, T>(0, null, null, queue);
		@SuppressWarnings("unused")
		private K nextK;
		private int currentModCount;
		private Entry<K, T> prevEntry = null;
		public ValuesIterator() {
			this.currentModCount = modCount;
			findNext();
		}
		
//		private void findNext() {
//			do {
//				while (next != null && next.nextE != null) {
//					nextT = next.nextE.getValue();
//					if (nextT != null) {
//						next = next.nextE;
//						return;
//					} else {
//						//delete
//						size--;
//						currentModCount = ++modCount;
//						next = next.nextE.nextE;
//					}
//				}
//				
//				pos++;
//				while (++pos < entries.length && entries[pos] == null) { };
//				if (pos < entries.length) {
//					newSlotDummy.nextE = entries[pos];
//					next = newSlotDummy;
//				} else {
//					next = null;
//				}
//			} while (next != null);
//		}
		
		private void findNext() {
			findNextUnsafe();
			while (next != null && (nextK = next.getKeyNotNull()) == null) {
				//remove
				newSlotDummy.nextE = next.nextE;
				WeakIdentityHashMapZ.this.remove(next);
				next = newSlotDummy;
				currentModCount = modCount;
				findNextUnsafe();
			}
		}
		
		private void findNextUnsafe() {
			if (next != null && next.nextE != null) {
				next = next.nextE;
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
			Entry<K, T> t = next;
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
			WeakIdentityHashMapZ.this.remove(prevEntry);
			currentModCount = modCount;
		}
	}


	private abstract class ResultSet<R> implements Set<R> {
		@Override
		public int size() {
			return WeakIdentityHashMapZ.this.size();
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


	@Override
	public boolean isEmpty() {
		return size() == 0;
	}
	
}

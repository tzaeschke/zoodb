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

import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

public class PrimLongMapZWeak<T> implements PrimLongMap<T> {

	public static class Entry<T> extends WeakReference<T> implements PrimLongEntry<T> {
		final long key;
		Entry<T> next = null;
		Entry(long key, T value) {
			super(value);
			this.key = key;
		}
		@Override
		public long getKey() {
			return key;
		}
		@Override
		public T getValue() {
			return super.get();
		}
	}
	
	private static final double LOAD_FACTOR = 0.75;
	
	private Entry<T>[] entries;
	
	private int capacityPower = 6;
	//capacity = 2^capacityPower
	private int limitMax = (int) ((1 << capacityPower) * LOAD_FACTOR);
	private int size = 0;
	
	private int modCount = 0;

	private Values valueResult;
	private EntrySet entryResult;
	private KeySet keyResult;
	
	public PrimLongMapZWeak() {
		this(60);
	}
	
	@SuppressWarnings("unchecked")
	public PrimLongMapZWeak(int capacity) {
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
	
//	private void printHisto() {
//		int[] histo = new int[size];
//		for (int i = 0; i < entries.length; i++) {
//			int n = 0;
//			Entry<T> e = entries[i];
//			while (e != null) {
//				n++;
//				e = e.next;
//			}
//			histo[n]++;
//		}
//		System.out.println("Histo: " + Arrays.toString(histo));
//	}
	
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
		//We do not clean up gc'd Entries here, this may cause ConcurrentModificationException
		return e == null ? null : e.getValue();
	}

	@Override
	public T put(long keyBits, T obj) {
		if (obj == null) {
			throw new IllegalArgumentException("Value must not be null.");
		}
		int pos = calcHash(keyBits);
		Entry<T> e = entries[pos];
		Entry<T> ePrev = null;
		while (e != null && e.key != keyBits) {
			ePrev = e;
			e = e.next;
		}
		modCount++;
		if (e != null) {
			//this works fine with weak refs
			Entry<T> eNew = new Entry<T>(e.key, obj);
			eNew.next = e.next;
			if (ePrev != null) {
				ePrev.next = eNew;
			} else {
				entries[pos] = eNew;
			}
			return e.getValue();
		}
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
			//this works fine with weak refs
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
				//This works fine with weak refs.
				//No cleaning up here to avoid concurrent modification.
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
		//this dummy represents an Entry whose 'next' is the first of a slot
		private final Entry<T> newSlotDummy = new Entry<T>(0, null);
		@SuppressWarnings("unused")
		private T nextT;
		private int currentModCount;
		public EntryIterator() {
			currentModCount = modCount;
			findNext();
		}
		
		private void findNext() {
			findNextUnsafe();
			while (next != null && (nextT = next.getValue()) == null) {
				//remove
				newSlotDummy.next = next.next;
				PrimLongMapZWeak.this.remove(next.key);
				next = newSlotDummy;
				currentModCount = modCount;
				findNextUnsafe();
			}
		}
		
		private void findNextUnsafe() {
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
		//this dummy represents an Entry whose 'next' is the first of a slot
		private final Entry<T> newSlotDummy = new Entry<T>(0, null);
		@SuppressWarnings("unused")
		private T nextT;
		private int currentModCount;
		public KeyIterator() {
			currentModCount = modCount;
			findNext();
		}
		
		private void findNext() {
			findNextUnsafe();
			while (next != null && (nextT = next.getValue()) == null) {
				//remove
				newSlotDummy.next = next.next;
				PrimLongMapZWeak.this.remove(next.key);
				next = newSlotDummy;
				currentModCount = modCount;
				findNextUnsafe();
			}
		}
		
		private void findNextUnsafe() {
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


	private class Values extends ResultSet<T> {

		@Override
		public Iterator<T> iterator() {
			return new ValuesIterator();
		}
	}
	
	class ValuesIterator implements Iterator<T> {
		private int pos = -1;
		private Entry<T> next;
		//this dummy represents an Entry whose 'next' is the first of a slot
		private final Entry<T> newSlotDummy = new Entry<T>(0, null);
		@SuppressWarnings("unused")
		private T nextT;
		private int currentModCount;
		private Entry<T> prevEntry = null;
		public ValuesIterator() {
			this.currentModCount = modCount;
			findNext();
		}
		
//		private void findNext() {
//			do {
//				while (next != null && next.next != null) {
//					nextT = next.next.getValue();
//					if (nextT != null) {
//						next = next.next;
//						return;
//					} else {
//						//delete
//						size--;
//						currentModCount = ++modCount;
//						next = next.next.next;
//					}
//				}
//				
//				pos++;
//				while (++pos < entries.length && entries[pos] == null) { };
//				if (pos < entries.length) {
//					newSlotDummy.next = entries[pos];
//					next = newSlotDummy;
//				} else {
//					next = null;
//				}
//			} while (next != null);
//		}
		
		private void findNext() {
			findNextUnsafe();
			while (next != null && (nextT = next.getValue()) == null) {
				//remove
				newSlotDummy.next = next.next;
				PrimLongMapZWeak.this.remove(next.key);
				next = newSlotDummy;
				currentModCount = modCount;
				findNextUnsafe();
			}
		}
		
		private void findNextUnsafe() {
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
			PrimLongMapZWeak.this.remove(prevEntry.key);
			currentModCount = modCount;
		}
	}


	private abstract class ResultSet<R> implements Set<R> {
		@Override
		public int size() {
			return PrimLongMapZWeak.this.size();
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
	
}

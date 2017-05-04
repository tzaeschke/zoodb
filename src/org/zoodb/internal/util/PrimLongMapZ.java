package org.zoodb.internal.util;

import java.util.Collection;
import java.util.Set;

public class PrimLongMapZ<T> implements PrimLongMap<T> {

	private static class Entry<T> implements PrimLongEntry<T> {
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
	}
	
	private static final double LOAD_FACTOR = 0.75;
	
	private Entry<T>[] entries;
	
	private int capacityPower = 6;
	//capacity = 2^capacityPower
	//TODO do we need a field for that?
	private int capacity = 1 << capacityPower;
	private int limitMax = (int) (capacity * LOAD_FACTOR);
	private int size = 0;
	
	private int modCount = 0;
	
	public PrimLongMapZ() {
		entries = new Entry[capacity];
	}
	
	
	private int calcHash(long key) {
		//This works well with primes:
		//return (int) key % capacity;
		//Knuths multiplicative hash function
		return (int)(key * 2654435761L) >> (32 - capacityPower);
	}
	
	private void checkRehash(int newSize) {
		if (newSize > limitMax && capacityPower < 31) {
			capacityPower++;
			capacity = 1 << capacityPower;
			limitMax = (int) (capacity * LOAD_FACTOR);
			Entry<T>[] oldEntries = entries;
			entries = new Entry[capacity];
			for (int i = 0; i < oldEntries.length; i++) {
				Entry<T> e = oldEntries[i];
				while (e != null) {
					putEntryNoCheck(e);
					e = e.next;
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
		return e == null ? null : e.value;
	}

	@Override
	public T put(long keyBits, T obj) {
		int pos = calcHash(keyBits);
		Entry<T> e = entries[pos];
		while (e != null && e.key != keyBits) {
			e = e.next;
		}
		modCount++;
		if (e != null) {
			T ret = e.value;
			e.value = obj;
			return ret;
		}
		checkRehash(size + 1);
		putEntryNoCheck(new Entry<T>(keyBits, obj));
		size++;
		return null;
	}

	@Override
	public T remove(long keyBits) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public Collection<T> values() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//
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
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return false;
	}

	@Override
	public void putAll(PrimLongMap<? extends T> map) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//
	}

	@Override
	public Set<Long> keySet() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public Set<PrimLongEntry<T>> entrySet() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

}

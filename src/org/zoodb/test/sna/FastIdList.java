package org.zoodb.test.sna;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * FastIdMap is a drop-in replacement for Map<Integer, ?> types.
 * It is basically an ArrayList whose index starts at 1. The ArrayList contains as many entries as
 * the value of the highest ID, therefore it is only efficient if most (or all) IDs have a value
 * assigned.
 * 
 * Implementing the Map interface and starting counting at 1 is simply done to allow compatibility
 * with existing SNA interfaces.
 * 
 * It is a lot faster and much more space efficient than normal Maps, because:
 * - it works with int primitive keys (put(), get() and general storage, thus avoiding auto boxing
 *   or any Integer instances at all.
 * - It doesn't require Map.Entry instances -> safes space
 * - It doesn't require expensive calls to polymorphic hashcode() and equals() methods.
 * 
 * 
 * 
 * @author Tilmann Zäschke
 *
 * @param <V>
 */
public class FastIdList<V> implements Collection<V> {

	private final ArrayList<V> list = new ArrayList<V>();
	private transient final FastIdMap map;

	public FastIdList() {
		map = new FastIdMap();
	}

	public V get(int id) {
		return list.get(id - 1);
	}
	
	public V put(int id, V value) {
		while (list.size() < id) {
			list.add(null);
		}
		return list.set(id - 1, value);
	}
	
	public Map asMap() {
		return map;
	}
	
	private class FastIdMap implements Map<Integer, V> {

		@Override
		public int size() {
			return list.size();
		}

		@Override
		public boolean isEmpty() {
			return list.isEmpty();
		}

		@Override
		public boolean containsKey(Object key) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean containsValue(Object value) {
			throw new UnsupportedOperationException();
		}

		@Override
		public V get(Object key) {
			return get((int)(Integer)key);
		}

		public V get(int pos) {
			return list.get(pos - 1);
		}

		@Override
		public V put(Integer key, V value) {
			return put((int)key, value);
		}

		public V put(int key, V value) {
			while (list.size() < key) {
				list.add(null);
			}
			return list.set(key-1, value);
		}

		@Override
		public V remove(Object key) {
			// set to null instead?
			throw new UnsupportedOperationException();
		}

		@Override
		public void putAll(Map<? extends Integer, ? extends V> m) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Set<Integer> keySet() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Collection<V> values() {
			return FastIdList.this;
		}

		@Override
		public Set<java.util.Map.Entry<Integer, V>> entrySet() {
			throw new UnsupportedOperationException();
		}

	}	

	@Override
	public boolean contains(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<V> iterator() {
		return list.iterator();
	}

	@Override
	public Object[] toArray() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object[] toArray(Object[] a) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean add(Object e) {
		return list.add((V) e);
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addAll(Collection c) {
		return list.addAll(c);
	}

	@Override
	public boolean removeAll(Collection c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean retainAll(Collection c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int size() {
		return list.size();
	}

	@Override
	public boolean isEmpty() {
		return list.isEmpty();
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		list.clear();
	}

}

package org.zoodb.jdo.api;

import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class DBHashtable<K, V> extends PersistenceCapableImpl implements Map<K, V>, DBCollection {

	private transient Hashtable<K, V> _t;
	
	public DBHashtable() {
		_t = new Hashtable<K, V>();
	}
	
	public DBHashtable(int size) {
		_t = new Hashtable<K, V>(size);
	}
	
	@Override
	public void clear() {
		_t.clear();
	}

	@Override
	public boolean containsKey(Object key) {
		return _t.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return _t.containsValue(value);
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return _t.entrySet();
	}

	@Override
	public V get(Object key) {
		return _t.get(key);
	}

	@Override
	public boolean isEmpty() {
		return _t.isEmpty();
	}

	@Override
	public Set<K> keySet() {
		return _t.keySet();
	}

	@Override
	public V put(K key, V value) {
		return _t.put(key, value);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		_t.putAll(m);
	}

	@Override
	public V remove(Object key) {
		return _t.remove(key);
	}

	@Override
	public int size() {
		return _t.size();
	}

	@Override
	public Collection<V> values() {
		return _t.values();
	}

	public void setBatchSize(int i) {
		System.out.println("STUB: DBHashtable.setBatchSize()");
	}

	public void resize(int size) {
		System.out.println("STUB: DBHashtable.resize()");
	}
}

package org.zoodb.jdo.api;

import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class DBHashtable<K, V> extends PersistenceCapableImpl implements Map<K, V>, DBCollection {

	private transient Hashtable<K, V> t;
	
	public DBHashtable() {
		t = new Hashtable<K, V>();
	}
	
	public DBHashtable(int size) {
		t = new Hashtable<K, V>(size);
	}
	
	@Override
	public void clear() {
		t.clear();
	}

	@Override
	public boolean containsKey(Object key) {
		return t.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return t.containsValue(value);
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return t.entrySet();
	}

	@Override
	public V get(Object key) {
		return t.get(key);
	}

	@Override
	public boolean isEmpty() {
		return t.isEmpty();
	}

	@Override
	public Set<K> keySet() {
		return t.keySet();
	}

	@Override
	public V put(K key, V value) {
		return t.put(key, value);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		t.putAll(m);
	}

	@Override
	public V remove(Object key) {
		return t.remove(key);
	}

	@Override
	public int size() {
		return t.size();
	}

	@Override
	public Collection<V> values() {
		return t.values();
	}

	public void setBatchSize(int i) {
		System.out.println("STUB: DBHashtable.setBatchSize()");
	}

	public void resize(int size) {
		System.out.println("STUB: DBHashtable.resize()");
	}
}

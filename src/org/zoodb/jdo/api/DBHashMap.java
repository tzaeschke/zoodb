/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.jdo.api;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

/**
 * Warning: This class does not track changes made to the valueSet(), entrySet() or keySet(). 
 * 
 * @author Tilmann Zäschke
 *
 * @param <K>
 * @param <V>
 */
public class DBHashMap<K, V> extends PersistenceCapableImpl implements Map<K, V>, DBCollection {

	private transient HashMap<K, V> t;
	
	public DBHashMap() {
		t = new HashMap<K, V>();
	}
	
	public DBHashMap(int size) {
		t = new HashMap<K, V>(size);
	}
	
	@Override
	public void clear() {
		zooActivateWrite();
		t.clear();
	}

	@Override
	public boolean containsKey(Object key) {
		zooActivateRead();
		return t.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		zooActivateRead();
		return t.containsValue(value);
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		zooActivateRead();
		return t.entrySet();
	}

	@Override
	public V get(Object key) {
		zooActivateRead();
		return t.get(key);
	}

	@Override
	public boolean isEmpty() {
		zooActivateRead();
		return t.isEmpty();
	}

	@Override
	public Set<K> keySet() {
		zooActivateRead();
		return t.keySet();
	}

	@Override
	public V put(K key, V value) {
		zooActivateWrite();
		return t.put(key, value);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		zooActivateWrite();
		t.putAll(m);
	}

	@Override
	public V remove(Object key) {
		zooActivateWrite();
		return t.remove(key);
	}

	@Override
	public int size() {
		zooActivateRead();
		return t.size();
	}

	@Override
	public Collection<V> values() {
		zooActivateRead();
		return t.values();
	}

	public void setBatchSize(int i) {
		System.out.println("STUB: DBHashtable.setBatchSize()");
	}

	public void resize(int size) {
		System.out.println("STUB: DBHashtable.resize()");
	}
}

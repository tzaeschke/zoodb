/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.api;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.zoodb.api.impl.ZooPCImpl;

/**
 * Warning: This class does not track changes made to the valueSet(), entrySet() or keySet(). 
 * 
 * @author Tilmann Zaeschke
 *
 * @param <K>
 * @param <V>
 */
public class DBHashMap<K, V> extends ZooPCImpl implements Map<K, V>, DBCollection {

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
	
	@Override
	public int hashCode() {
        int h = 0;
        for (K k: keySet()) {
        	if (k != null && !(k instanceof DBCollection)) {
        		h += k.hashCode();
        	}
        }
        for (V v: values()) {
        	if (v != null && !(v instanceof DBCollection)) {
        		h += v.hashCode();
        	}
        }
        return h;
        //TODO: For some reason the following fails some tests.... (014/015)
//		return (int) (jdoZooGetOid()*10000) | size();  
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof DBHashMap)) {
			return false;
		}
		DBHashMap<?, ?> m = (DBHashMap<?, ?>) obj;
		if (size() != m.size() || jdoZooGetOid() != m.jdoZooGetOid()) {
			return false;
		}
        try {
            Iterator<Entry<K,V>> i = entrySet().iterator();
            while (i.hasNext()) {
                Entry<K,V> e = i.next();
                K key = e.getKey();
                V value = e.getValue();
                if (value == null) {
                    if (!(m.get(key)==null && m.containsKey(key)))
                        return false;
                } else {
                	Object v2 = m.get(key);
                	if (value != v2 && !value.equals(v2))
                        return false;
                }
            }
        } catch (ClassCastException unused) {
            return false;
        } catch (NullPointerException unused) {
            return false;
        }
//		for (Map.Entry<K, V> e: entrySet()) {
//			Object v2 = o.get(e.getKey());
//			if ((e.getValue() == null && v2 != null) || !e.getValue().equals(v2)) {
//				return false;
//			}
//		}
		return true;
	}
}

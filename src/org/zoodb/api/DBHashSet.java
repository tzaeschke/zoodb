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
package org.zoodb.api;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.zoodb.api.impl.ZooPC;

public class DBHashSet<E> extends ZooPC implements Set<E>, DBCollection {
	
	private transient HashSet<E> v;

	public DBHashSet() {
		v = new HashSet<E>();
	}
	
	public DBHashSet(int initialCapacity) {
		v = new HashSet<E>(initialCapacity);
	}
	
	@Override
	public boolean add(E e) {
		zooActivateWrite();
		return v.add(e);
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		zooActivateWrite();
		return v.addAll(c);
	}

	@Override
	public void clear() {
		zooActivateWrite();
		v.clear();
	}

	@Override
	public boolean contains(Object o) {
		zooActivateRead();
		return v.contains(o);
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		zooActivateRead();
		return v.containsAll(c);
	}

	@Override
	public boolean isEmpty() {
		zooActivateRead();
		return v.isEmpty();
	}

	@Override
	public Iterator<E> iterator() {
		zooActivateRead();
		return new DBIterator(v.iterator());
	}

	@Override
	public boolean remove(Object o) {
		zooActivateWrite();
		return v.remove(o);
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		zooActivateWrite();
		return v.removeAll(c);
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		zooActivateWrite();
		return v.retainAll(c);
	}

	@Override
	public int size() {
		zooActivateRead();
		return v.size();
	}

	@Override
	public Object[] toArray() {
		zooActivateRead();
		return v.toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		zooActivateRead();
		return v.toArray(a);
	}
	
	private class DBIterator implements Iterator<E> {

		private final Iterator<E> iter;
		
		private DBIterator(Iterator<E> iter) {
			this.iter = iter;
		}
		
		@Override
		public boolean hasNext() {
			return iter.hasNext();
		}

		@Override
		public E next() {
			return iter.next();
		}

		@Override
		public void remove() {
			zooActivateWrite();
			iter.remove();
		}
		
	}
	
	@Override
	public int hashCode() {
		return (int) (jdoZooGetOid()*10000) | size();  
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof DBHashSet)) {
			return false;
		}
		DBHashSet<?> o = (DBHashSet<?>) obj;
		if (size() != o.size() || jdoZooGetOid() != o.jdoZooGetOid()) {
			return false;
		}
		for (E e : this)
			if (!o.contains(e))
				return false;
		return true;
	}
}

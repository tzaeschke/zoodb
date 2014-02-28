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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.zoodb.api.impl.ZooPCImpl;

public class DBArrayList<E> extends ZooPCImpl implements List<E>, DBCollection {
	
	private transient ArrayList<E> v;

	public DBArrayList() {
		v = new ArrayList<E>();
	}
	
	public DBArrayList(int size) {
		v = new ArrayList<E>(size);
	}
	
	@Override
	public boolean add(E e) {
		zooActivateWrite();
		return v.add(e);
	}

	@Override
	public void add(int index, E element) {
		zooActivateWrite();
		v.add(index, element);
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		zooActivateWrite();
		return v.addAll(c);
	}

	@Override
	public boolean addAll(int index, Collection<? extends E> c) {
		zooActivateWrite();
		return v.addAll(index, c);
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
	public E get(int index) {
		zooActivateRead();
		return v.get(index);
	}

	@Override
	public int indexOf(Object o) {
		zooActivateRead();
		return v.indexOf(o);
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
	public int lastIndexOf(Object o) {
		zooActivateRead();
		return v.lastIndexOf(o);
	}

	@Override
	public ListIterator<E> listIterator() {
		zooActivateRead();
		return new DBListIterator(v.listIterator());
	}

	@Override
	public ListIterator<E> listIterator(int index) {
		zooActivateRead();
		return new DBListIterator(v.listIterator(index));
	}

	@Override
	public boolean remove(Object o) {
		zooActivateWrite();
		return v.remove(o);
	}

	@Override
	public E remove(int index) {
		zooActivateWrite();
		return v.remove(index);
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
	public E set(int index, E element) {
		zooActivateWrite();
		return v.set(index, element);
	}

	@Override
	public int size() {
		zooActivateRead();
		return v.size();
	}

	@Override
	public List<E> subList(int fromIndex, int toIndex) {
		zooActivateRead();
		return v.subList(fromIndex, toIndex);
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

	public void setBatchSize(int i) {
		System.out.println("STUB: DBVector.setBatchSize()");
	}

	public void resize(int size) {
		System.out.println("STUB: DBVector.resize()");
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
	private class DBListIterator extends DBArrayList<E>.DBIterator implements ListIterator<E> {

		private final ListIterator<E> iter;
		
		private DBListIterator(ListIterator<E> iter) {
			super(iter);
			this.iter = iter;
		}

		@Override
		public boolean hasPrevious() {
			return iter.hasPrevious();
		}

		@Override
		public E previous() {
			return iter.previous();
		}

		@Override
		public int nextIndex() {
			return iter.nextIndex();
		}

		@Override
		public int previousIndex() {
			return iter.previousIndex();
		}

		@Override
		public void set(E e) {
			zooActivateWrite();
			iter.set(e);
		}

		@Override
		public void add(E e) {
			zooActivateWrite();
			iter.add(e);
		}
		
	}
	
	@Override
	public int hashCode() {
		return (int) (jdoZooGetOid()*10000) | size();  
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof DBArrayList)) {
			return false;
		}
		DBArrayList<?> o = (DBArrayList<?>) obj;
		if (size() != o.size() || jdoZooGetOid() != o.jdoZooGetOid()) {
			return false;
		}
		for (int i = 0; i < size(); i++) {
			if (!get(i).equals(o.get(i))) {
				return false;
			}
		}
		return true;
	}
}

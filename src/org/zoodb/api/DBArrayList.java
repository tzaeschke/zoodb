/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zoodb.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.zoodb.api.impl.ZooPC;

public class DBArrayList<E> extends ZooPC implements List<E>, DBCollection {
	
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

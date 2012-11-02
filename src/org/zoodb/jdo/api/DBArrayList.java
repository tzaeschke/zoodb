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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class DBArrayList<E> extends PersistenceCapableImpl implements List<E>, DBCollection {
	
	private transient ArrayList<E> v;

	public DBArrayList() {
		v = new ArrayList<E>();
	}
	
	public DBArrayList(int size) {
		v = new ArrayList<E>(size);
	}
	
	@Override
	public boolean add(E e) {
		activateWrite("v");
		return v.add(e);
	}

	@Override
	public void add(int index, E element) {
		activateWrite("v");
		v.add(index, element);
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		activateWrite("v");
		return v.addAll(c);
	}

	@Override
	public boolean addAll(int index, Collection<? extends E> c) {
		activateWrite("v");
		return v.addAll(index, c);
	}

	@Override
	public void clear() {
		activateWrite("v");
		v.clear();
	}

	@Override
	public boolean contains(Object o) {
		activateRead("v");
		return v.contains(o);
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		activateRead("v");
		return v.containsAll(c);
	}

	@Override
	public E get(int index) {
		activateRead("v");
		return v.get(index);
	}

	@Override
	public int indexOf(Object o) {
		activateRead("v");
		return v.indexOf(o);
	}

	@Override
	public boolean isEmpty() {
		activateRead("v");
		return v.isEmpty();
	}

	@Override
	public Iterator<E> iterator() {
		activateRead("v");
		return new DBIterator(v.iterator());
	}

	@Override
	public int lastIndexOf(Object o) {
		activateRead("v");
		return v.lastIndexOf(o);
	}

	@Override
	public ListIterator<E> listIterator() {
		activateRead("v");
		return new DBListIterator(v.listIterator());
	}

	@Override
	public ListIterator<E> listIterator(int index) {
		activateRead("v");
		return new DBListIterator(v.listIterator(index));
	}

	@Override
	public boolean remove(Object o) {
		activateWrite("v");
		return v.remove(o);
	}

	@Override
	public E remove(int index) {
		activateWrite("v");
		return v.remove(index);
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		activateWrite("v");
		return v.removeAll(c);
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		activateWrite("v");
		return v.retainAll(c);
	}

	@Override
	public E set(int index, E element) {
		activateWrite("v");
		return v.set(index, element);
	}

	@Override
	public int size() {
		activateRead("v");
		return v.size();
	}

	@Override
	public List<E> subList(int fromIndex, int toIndex) {
		activateRead("v");
		return v.subList(fromIndex, toIndex);
	}

	@Override
	public Object[] toArray() {
		activateRead("v");
		return v.toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		activateRead("v");
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
			activateWrite("v");
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
			activateWrite("v");
			iter.set(e);
		}

		@Override
		public void add(E e) {
			activateWrite("v");
			iter.add(e);
		}
		
	}
}

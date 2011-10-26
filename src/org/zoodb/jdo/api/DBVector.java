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
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Vector;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class DBVector<E> extends PersistenceCapableImpl implements List<E>, DBCollection {
	private transient Vector<E> v;

	public DBVector() {
		v = new Vector<E>();
	}
	
	public DBVector(int size) {
		v = new Vector<E>(size);
	}
	
	@Override
	public boolean add(E e) {
		return v.add(e);
	}

	@Override
	public void add(int index, E element) {
		v.add(index, element);
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		return v.addAll(c);
	}

	@Override
	public boolean addAll(int index, Collection<? extends E> c) {
		return v.addAll(index, c);
	}

	@Override
	public void clear() {
		v.clear();
	}

	@Override
	public boolean contains(Object o) {
		return v.contains(o);
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return v.containsAll(c);
	}

	@Override
	public E get(int index) {
		return v.get(index);
	}

	@Override
	public int indexOf(Object o) {
		return v.indexOf(o);
	}

	@Override
	public boolean isEmpty() {
		return v.isEmpty();
	}

	@Override
	public Iterator<E> iterator() {
		return v.iterator();
	}

	@Override
	public int lastIndexOf(Object o) {
		return v.lastIndexOf(o);
	}

	@Override
	public ListIterator<E> listIterator() {
		return v.listIterator();
	}

	@Override
	public ListIterator<E> listIterator(int index) {
		return v.listIterator(index);
	}

	@Override
	public boolean remove(Object o) {
		return v.remove(o);
	}

	@Override
	public E remove(int index) {
		return v.remove(index);
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		return v.removeAll(c);
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		return v.retainAll(c);
	}

	@Override
	public E set(int index, E element) {
		return v.set(index, element);
	}

	@Override
	public int size() {
		return v.size();
	}

	@Override
	public List<E> subList(int fromIndex, int toIndex) {
		return v.subList(fromIndex, toIndex);
	}

	@Override
	public Object[] toArray() {
		return v.toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return v.toArray(a);
	}

	public void setBatchSize(int i) {
		System.out.println("STUB: DBVector.setBatchSize()");
	}

	public void resize(int size) {
		System.out.println("STUB: DBVector.resize()");
	}
}

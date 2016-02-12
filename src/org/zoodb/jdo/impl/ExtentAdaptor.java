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
package org.zoodb.jdo.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import javax.jdo.Extent;

public class ExtentAdaptor<E> implements Collection<E> {

	private final Extent<E> ext;
	private ArrayList<E> materializedList = null;
	
	ExtentAdaptor(Extent<E> extent) {
		ext = extent;
	}

	private ArrayList<E> materialize() {
		//WARNING: this loads all instances into memory!
		if (materializedList == null) {
			materializedList = new ArrayList<>();
			for (E e: this) {
				materializedList.add(e);
			}
		}
		return materializedList;
	}
	
	@Override
	public int size() {
		return materialize().size();
	}

	@Override
	public boolean isEmpty() {
		return !iterator().hasNext();
	}

	@Override
	public boolean contains(Object o) {
		return materialize().contains(o);
	}

	@Override
	public Iterator<E> iterator() {
		return ext.iterator();
	}

	@Override
	public Object[] toArray() {
		return materialize().toArray();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object[] toArray(Object[] a) {
		return materialize().toArray(a);
	}

	@Override
	public boolean add(Object e) {
		throw new UnsupportedOperationException("Extents are immutable.");
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException("Extents are immutable.");
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean containsAll(Collection c) {
		return materialize().containsAll(c);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean addAll(Collection c) {
		throw new UnsupportedOperationException("Extents are immutable.");
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean removeAll(Collection c) {
		throw new UnsupportedOperationException("Extents are immutable.");
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean retainAll(Collection c) {
		throw new UnsupportedOperationException("Extents are immutable.");
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException("Extents are immutable.");
	}

	public void closeAll() {
		ext.closeAll();
	}
}

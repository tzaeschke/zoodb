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

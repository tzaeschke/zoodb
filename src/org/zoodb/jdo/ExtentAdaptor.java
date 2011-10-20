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
package org.zoodb.jdo;

import java.util.Collection;
import java.util.Iterator;

import javax.jdo.Extent;

public class ExtentAdaptor<E> implements Collection<E> {

	private final Extent<E> _ext;
	
	ExtentAdaptor(Extent<E> extent) {
		_ext = extent;
	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return 0;
	}

	@Override
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return false;
	}

	@Override
	public boolean contains(Object o) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return false;
	}

	@Override
	public Iterator<E> iterator() {
		return _ext.iterator();
	}

	@Override
	public Object[] toArray() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object[] toArray(Object[] a) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public boolean add(Object e) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return false;
	}

	@Override
	public boolean remove(Object o) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return false;
	}

	@Override
	public boolean containsAll(Collection c) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return false;
	}

	@Override
	public boolean addAll(Collection c) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return false;
	}

	@Override
	public boolean removeAll(Collection c) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return false;
	}

	@Override
	public boolean retainAll(Collection c) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return false;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//
	}
}

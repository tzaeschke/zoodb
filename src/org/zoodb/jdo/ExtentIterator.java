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

import java.util.Iterator;
import java.util.NoSuchElementException;

public class ExtentIterator<E> implements Iterator<E> {

	//_it==null means closed
	private Iterator<E> _it;
	
	ExtentIterator(Iterator<E> it) {
		_it = it;
	}
	
	@Override
	public boolean hasNext() {
		if (_it == null) {
			return false;
		}
		return _it.hasNext();
	}

	@Override
	public E next() {
		if (_it == null) {
			throw new NoSuchElementException("This iterator has been closed.");
		}
		return _it.next();
	}

	@Override
	public void remove() {
		if (_it == null) {
			throw new NoSuchElementException("This iterator has been closed.");
		}
		_it.remove();
	}

	public void close() {
		_it = null;
	}
}

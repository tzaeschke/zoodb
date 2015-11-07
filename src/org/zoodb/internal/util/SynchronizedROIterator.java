/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.internal.util;

import java.util.Iterator;


/**
 * Closeable synchronized read-only iterator.
 * 
 * @author ztilmann
 *
 */
public class SynchronizedROIterator<E> implements Iterator<E> {

	private final Iterator<E> i;
	private final ClientLock lock;
	
	
	public SynchronizedROIterator(Iterator<E> i, ClientLock lock) {
		this.i = i;
		this.lock = lock;
	}
	


	@Override
	public boolean hasNext() {
		try {
			lock.lock();
			return this.i.hasNext();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public E next() {
		try {
			lock.lock();
			return this.i.next();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("This collection is unmodifiable.");
	}
}

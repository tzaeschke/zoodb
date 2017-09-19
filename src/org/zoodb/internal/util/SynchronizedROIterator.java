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
package org.zoodb.internal.util;

import java.util.NoSuchElementException;


/**
 * Closeable synchronized read-only iterator.
 * 
 * @author ztilmann
 */
public class SynchronizedROIterator<E> implements CloseableIterator<E> {

	private final CloseableIterator<E> i;
	private final ClientLock lock;
	
	//TODO this is really bad and should happen on the server...
	private int maxExcl;
	private int posOfNext = 0;
	
	
	public SynchronizedROIterator(CloseableIterator<E> i, ClientLock lock) {
		this(i, lock, 0, Integer.MAX_VALUE);
	}
	

	public SynchronizedROIterator(CloseableIterator<E> i, ClientLock lock, int minIncl, int maxExcl) {
		this.i = i;
		this.lock = lock;
		this.maxExcl = maxExcl > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) maxExcl;
		while (posOfNext < minIncl && i.hasNext()) {
			//TODO argh!!!
			i.next();
			posOfNext++;
		}
	}
	

	@Override
	public void close() {
		try {
			lock.lock();
			this.i.close();
		} finally {
			lock.unlock();
		}
	}


	@Override
	public boolean hasNext() {
		try {
			lock.lock();
			return posOfNext < maxExcl && this.i.hasNext();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public E next() {
		try {
			lock.lock();
			posOfNext++;
			if (posOfNext > maxExcl) {
				throw new NoSuchElementException();
			}
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

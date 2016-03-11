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


/**
 * Closeable synchronized read-only iterator.
 * 
 * @author ztilmann
 *
 */
public class SynchronizedROIteratorC<E> extends SynchronizedROIterator<E> 
implements CloseableIterator<E> {

	private final CloseableIterator<E> i;
	private final ClientLock lock;
	
	
	public SynchronizedROIteratorC(CloseableIterator<E> i, ClientLock lock) {
		super(i, lock, 0, Integer.MAX_VALUE);
		this.i = i;
		this.lock = lock;
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

}

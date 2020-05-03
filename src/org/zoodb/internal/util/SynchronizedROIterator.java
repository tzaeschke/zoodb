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
package org.zoodb.internal.util;

import java.util.NoSuchElementException;


/**
 * Closeable synchronized read-only iterator.
 * 
 * @author ztilmann
 * 
 * @param <E> Value type
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
		this.maxExcl = maxExcl;
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

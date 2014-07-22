/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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

import java.util.concurrent.Semaphore;

public class RWSemaphore<T> {

	private static final int MAX_READERS = 16;
	private final T NO_KEY = null;
	
	private final Semaphore rSemaphore;
	private final Semaphore wSemaphore;
	private T currentWriterKey = NO_KEY;
	
	public RWSemaphore(boolean fair) {
		rSemaphore = new Semaphore(MAX_READERS, fair);
		wSemaphore = new Semaphore(1, fair);
	}
	
	public void readLock(T key) {
		try {
			wSemaphore.acquire();
			rSemaphore.acquire();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} finally {
			wSemaphore.release();
		}
	}
	
	public void writeLock(T key) {
		//ensure that there is no writer yet
		try {
			wSemaphore.acquire();
			if (currentWriterKey != NO_KEY) {
				throw new IllegalStateException();
			}
			currentWriterKey = key;
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	private void releaseRead(T key) {
		rSemaphore.release();
	}
	
	private void releaseWrite(T key) {
		if (currentWriterKey != key) {
			throw new IllegalStateException();
		}
		currentWriterKey = NO_KEY;
		wSemaphore.release();
	}
	
	public void release(T key) {
		if (currentWriterKey == key) {
			releaseWrite(key);
		} else {
			releaseRead(key);
		}
	}
	
}

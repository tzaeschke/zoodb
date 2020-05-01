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

import java.util.concurrent.Semaphore;

public class RWSemaphore<T> {

	public static final int MAX_READERS = 16;
	private final T NO_KEY = null;
	
	private final Semaphore rSemaphore;
	private final Semaphore wSemaphore;
	private T currentWriterKey = NO_KEY;
	
	public RWSemaphore() {
		rSemaphore = new Semaphore(MAX_READERS, false);
		wSemaphore = new Semaphore(1, false);
	}
	
	public void readLock(T key) {
		try {
			wSemaphore.acquire();
			rSemaphore.acquire();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
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
			//wait for other readers to finish
			rSemaphore.acquire(MAX_READERS);
			rSemaphore.release(MAX_READERS);
			currentWriterKey = key;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		}
	}
	
	private void releaseRead(T key) {
		if (rSemaphore.availablePermits() == MAX_READERS) {
			// i.e. there are no locks left to be released.
			throw new IllegalStateException(); 
//			new IllegalStateException().printStackTrace();
//			return;
		}
		rSemaphore.release();
	}
	
	private void releaseWrite(T key) {
		if (currentWriterKey != key) {
			//throw 
			new IllegalStateException().printStackTrace();
			return;
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

	public boolean isLocked() {
		return rSemaphore.availablePermits() < MAX_READERS || wSemaphore.availablePermits() < 1 ;
	}
	
}

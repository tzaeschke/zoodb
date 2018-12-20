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

import org.zoodb.internal.server.DiskAccess;

public class RWSemaphoreSync<T> {

	public static final int MAX_READERS = 16;
	private final T NO_KEY = null;
	
	
	private volatile int rSemaphore;
	private volatile int wSemaphore;
	private volatile T currentWriterKey = NO_KEY;
//	private ConcurrentHashMap<T, Object> readers = new ConcurrentHashMap<>(MAX_READERS);
	
	public RWSemaphoreSync() {
		rSemaphore = 0;
		wSemaphore = 0;
	}
	
	public void readLock(T key) {
		synchronized (this) {
			while (rSemaphore >= MAX_READERS || wSemaphore > 0) {
				this.waitSafe();
			}
			rSemaphore++;
			this.notify();
		}
	}
	
	public void writeLock(T key) {
		//ensure that there is no writer yet
		synchronized (this) {
			while (rSemaphore > 0 || wSemaphore > 0) {
				this.waitSafe();
			}
			wSemaphore++;
			currentWriterKey = key;
			this.notify();
		}
	}
	
	private void waitSafe() {
		try {
			wait();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		}
	}
	
	private void releaseRead(T key) {
		synchronized (this) {
			if (rSemaphore <= 0) {
				// i.e. there are no locks left to be released.
				throw new IllegalStateException(); 
//				new IllegalStateException().printStackTrace();
//				return;
			}
			rSemaphore--;
			this.notify();
		}
	}
	
	private void releaseWrite(T key) {
		synchronized (this) {
			if (wSemaphore <= 0) {
				// i.e. there are no locks left to be released.
				throw new IllegalStateException(); 
//				new IllegalStateException().printStackTrace();
//				return;
			}
			if (currentWriterKey != key) {
				//throw 
				new IllegalStateException().printStackTrace();
				return;
			}
			currentWriterKey = NO_KEY;
			wSemaphore--;
			this.notify();
		}
	}
	
	public void release(T key) {
		if (currentWriterKey == key) {
			releaseWrite(key);
		} else {
			releaseRead(key);
		}
	}

	public boolean isLocked() {
		synchronized (this) {
			return rSemaphore > 0 || wSemaphore > 0;
		}
	}

	public boolean isLocked(T key) {
		synchronized (this) {
			return currentWriterKey == key 
					|| (wSemaphore == 0 && rSemaphore > 0);
		}
	}
	
	public void assertLocked(T key) {
		synchronized (this) {
			if (!isLocked(key)) {
				String msg = "W=" + currentWriterKey + "/" + key + "/" 
						+ wSemaphore +"; R=" + rSemaphore;
				System.err.println("Uncontrolled DB access! xx " + msg);
//				RuntimeException e = DBLogger.newFatal("Uncontrolled DB access!" + msg);
//				e.printStackTrace();
//				throw e;
			}
		}
//		if (rSemaphore.availablePermits() < MAX_READERS && !readers.containsKey(key)) {
//			throw DBLogger.newFatal("Uncontrolled DB access!");
//		}
	}

	public void assertWLocked(DiskAccess key) {
		synchronized (this) {
			if (currentWriterKey != key) {
				String msg = "W=" + currentWriterKey + "/" + key + "/" 
						+ wSemaphore +"; R=" + rSemaphore;
				System.err.println("Uncontrolled DB WRITE access! xx " + msg);
//				RuntimeException e = DBLogger.newFatal("Uncontrolled DB access!" + msg);
//				e.printStackTrace();
//				throw e;
			}
		}
	}
}

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

import java.util.concurrent.locks.ReentrantLock;

/**
 * A lock for the client (session, queries, activate(), ....) to better support multi-threaded
 * access to a single session.
 * @author ztilmann
 *
 */
public class ClientLock {

//	private final Semaphore lock = new Semaphore(1);
//	
//	public void lock() {
//		try {
//			lock.acquire();
//		} catch (InterruptedException e) {
//			Thread.currentThread().interrupt();
//		}
//	}
//	
//	public void unlock() {
//		lock.release();
//	}
	
	private final ReentrantLock lock = new ReentrantLock();
	private boolean isLockingEnabled = true;
	{
		lock.lock();
	}
	
	public void lock() {
		if (isLockingEnabled) {
			lock.lock();
		}
	}
	
	public void unlock() {
		if (isLockingEnabled) {
			lock.unlock();
		}
	}

	public boolean isLockingEnabled() {
		return isLockingEnabled;
	}

	public void enableLocking(boolean enable) {
		//This is not threadsafe, however, if we ever call this method
		//we are apparently not interested in thread safety anyway...
		isLockingEnabled = enable;
		if (!enable && lock.isHeldByCurrentThread()) {
			lock.unlock();
		}
	}
}

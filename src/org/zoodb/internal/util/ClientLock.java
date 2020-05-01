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

	public boolean isLocked() {
		return lock.isLocked();
	}

	public boolean isLockedByCurrentThread() {
		return lock.isHeldByCurrentThread();
	}

	public boolean isLockingEnabled() {
		return isLockingEnabled;
	}

	public void enableLocking(boolean enable) {
		//This is not threadsafe, however, if we ever call this method
		//we are apparently not interested in thread safety anyway...
		isLockingEnabled = enable;
		if (!enable && lock.isHeldByCurrentThread()) {
			while (lock.getHoldCount() > 0) {
				lock.unlock();
			}
		}
	}
}

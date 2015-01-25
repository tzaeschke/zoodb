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
package org.zoodb.test.util;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.zoodb.internal.util.RWSemaphore;

public class RWSemaphoreTest {

	@Test
	public void testWafterW() throws InterruptedException {
		RWSemaphore<Object> s = new RWSemaphore<>();
		
		GetWLOCK w1 = new GetWLOCK(s);
		w1.start();
		//get WLOCK should be fine
		w1.join();
		
		//try 2nd W-lock
		GetWLOCK w2 = new GetWLOCK(s);
		w2.start();
		Thread.sleep(100);
		//get WLOCK should be waiting
		
		assertTrue(w2.isAlive());
		
		//attempt wrong unlock
		try {
			s.release(w2);
			fail();
		} catch (IllegalStateException e) {
			//can't unlock with wrong key
		}
		
		//now unlock 1 and wait for 2
		s.release(w1);
		
		//now wait for w2
		w2.join();
		
		s.release(w2);
	}
	
	@Test
	public void testWafterR() throws InterruptedException {
		RWSemaphore<Object> s = new RWSemaphore<>();
		
		GetRLOCK r1 = new GetRLOCK(s);
		r1.start();
		//get RLOCK should be fine
		r1.join();
		
		//try 2nd W-lock
		GetWLOCK w2 = new GetWLOCK(s);
		w2.start();
		Thread.sleep(100);
		//get WLOCK should be waiting
		
		assertTrue(w2.isAlive());
		
		//attempt wrong unlock
		//TODO lock release is currently not key-sensitive if rlocks are present
//		try {
//			s.release(w2);
//			fail();
//		} catch (IllegalStateException e) {
//			//can't unlock with wrong key
//		}
		
		//now unlock 1 and wait for 2
		s.release(r1);
		
		//now wait for w2
		w2.join();
		
		s.release(w2);
	}
	
	@Test
	public void testRafterW() throws InterruptedException {
		RWSemaphore<Object> s = new RWSemaphore<>();
		
		GetWLOCK w1 = new GetWLOCK(s);
		w1.start();
		//get WLOCK should be fine
		w1.join();
		
		//try R-lock
		GetRLOCK r2 = new GetRLOCK(s);
		r2.start();
		Thread.sleep(100);
		//get RLOCK should be waiting
		
		assertTrue(r2.isAlive());
		
		//attempt wrong unlock
		try {
			s.release(r2);
			fail();
		} catch (IllegalStateException e) {
			//can't unlock with wrong key
		}
		
		//now unlock 1 and wait for 2
		s.release(w1);
		
		//now wait for w2
		r2.join();
		
		s.release(r2);
	}
	
	@Test
	public void testRafterR() throws InterruptedException {
		RWSemaphore<Object> s = new RWSemaphore<>();
		
		GetRLOCK[] r = new GetRLOCK[RWSemaphore.MAX_READERS];
		for (int i = 0; i < r.length; i++) {
			r[i] = new GetRLOCK(s);
			r[i].start();
			//get RLOCK should be fine
			r[i].join();
		}
		
		//try 2nd R-lock
		GetRLOCK r2 = new GetRLOCK(s);
		r2.start();
		Thread.sleep(100);
		//get WLOCK should be waiting
		
		assertTrue(r2.isAlive());
		
		//attempt wrong unlock
		//TODO R-lock release is currently not key-sensitive
//		try {
//			s.release(r2);
//			fail();
//		} catch (IllegalStateException e) {
//			//can't unlock with wrong key
//		}
		
		//now unlock 1 and wait for 2
		s.release(r[3]);
		
		//now wait for w2
		r2.join();
		
		s.release(r2);
		for (int i = 0; i < r.length; i++) {
			if (i != 3) {
				s.release(r[i]);
			}
		}
	}
	
	private static class GetRLOCK extends Thread {
		final RWSemaphore<Object> lock;
		public GetRLOCK(RWSemaphore<Object> lock) {
			this.lock = lock;
		}
		public void run() {
			lock.readLock(this);
		}
	}
	
	private static class GetWLOCK extends Thread {
		final RWSemaphore<Object> lock;
		public GetWLOCK(RWSemaphore<Object> lock) {
			this.lock = lock;
		}
		public void run() {
			lock.writeLock(this);
		}
	}
	
}

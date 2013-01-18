/*
 * Copyright 2009-2012 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.test;

import static org.junit.Assert.*;

import javax.jdo.PersistenceManager;
import javax.jdo.listener.ClearCallback;
import javax.jdo.listener.DeleteCallback;
import javax.jdo.listener.LoadCallback;
import javax.jdo.listener.StoreCallback;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.test.util.TestTools;

public class Test_045_TransactionsCallBacks {

	private static boolean loadCalled = false;
	private static boolean storeCalled = false;
	private static boolean clearCalled = false;
	private static boolean deleteCalled = false;
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb();
		TestTools.defineSchema(TestPostLoad.class, TestPreStore.class, TestPreClear.class, 
				TestPreDelete.class);
	}

	@Before
	public void before() {
		TestTools.dropInstances(TestPostLoad.class, TestPreStore.class, TestPreClear.class,
				TestPreDelete.class);
	}

	static class TestPostLoad extends PersistenceCapableImpl implements LoadCallback {
		int i = 1;
		@Override
		public void jdoPostLoad() {
			assertTrue(i == 1);
			assertFalse(loadCalled);
			loadCalled = true;
		}
		int getI() {
			zooActivateRead();
			return i;
		}
	}
	
	static class TestPreStore extends PersistenceCapableImpl implements StoreCallback {
		int i = 1;
		@Override
		public void jdoPreStore() {
			storeCalled = true;
		}
	}
	
	static class TestPreClear extends PersistenceCapableImpl implements ClearCallback {
		int i = 1;
		@Override
		public void jdoPreClear() {
			clearCalled = true;
		}
	}
	
	static class TestPreDelete extends PersistenceCapableImpl implements DeleteCallback {
		int i = 1;
		@Override
		public void jdoPreDelete() {
			deleteCalled = true;
		}
	}
	
	@Test
	public void testCallBack() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestPreStore ps = new TestPreStore();
		TestPostLoad pl = new TestPostLoad();
		TestPreClear pc = new TestPreClear();
		TestPreDelete pd = new TestPreDelete(); 
		
		pm.makePersistent(ps);
		pm.makePersistent(pl);
		pm.makePersistent(pc);
		pm.makePersistent(pd);
		
		assertFalse(storeCalled);
		assertFalse(loadCalled);
		assertFalse(clearCalled);
		assertFalse(deleteCalled);
		
		pm.currentTransaction().commit();
		
		assertTrue(storeCalled);
		assertTrue(clearCalled);
		assertFalse(loadCalled);
		assertFalse(deleteCalled);
		
		pm.currentTransaction().begin();
		
		pl.getI();
		assertTrue(loadCalled);
		
		pm.deletePersistent(pd);
		assertFalse(deleteCalled);
		
		pm.currentTransaction().commit();
		assertTrue(deleteCalled);
		
		pm.close();
	}
	
	@After
	public void afterTest() {
		TestTools.closePM();
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
	}
}

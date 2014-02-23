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
import org.zoodb.test.testutil.TestTools;

public class Test_045_TransactionsCallBacks {

	
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
		
		assertFalse(TestPreStore.storeCalled);
		assertFalse(TestPostLoad.loadCalled);
		assertFalse(TestPreClear.clearCalled);
		assertFalse(TestPreDelete.deleteCalled);
		
		pm.currentTransaction().commit();
		
		assertTrue(TestPreStore.storeCalled);
		assertTrue(TestPreClear.clearCalled);
		assertFalse(TestPostLoad.loadCalled);
		assertFalse(TestPreDelete.deleteCalled);
		
		pm.currentTransaction().begin();
		
		pl.getI();
		assertTrue(TestPostLoad.loadCalled);
		
		pm.deletePersistent(pd);
		assertFalse(TestPreDelete.deleteCalled);
		
		pm.currentTransaction().commit();
		assertTrue(TestPreDelete.deleteCalled);
		
		TestTools.closePM();
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

class TestPostLoad extends PersistenceCapableImpl implements LoadCallback {
	static boolean loadCalled = false;
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

class TestPreStore extends PersistenceCapableImpl implements StoreCallback {
	static boolean storeCalled = false;
	int i = 1;
	@Override
	public void jdoPreStore() {
		storeCalled = true;
	}
}

class TestPreClear extends PersistenceCapableImpl implements ClearCallback {
	static boolean clearCalled = false;
	int i = 1;
	@Override
	public void jdoPreClear() {
		clearCalled = true;
	}
}

class TestPreDelete extends PersistenceCapableImpl implements DeleteCallback {
	static boolean deleteCalled = false;
	int i = 1;
	@Override
	public void jdoPreDelete() {
		deleteCalled = true;
	}
}



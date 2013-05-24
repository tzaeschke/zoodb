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
import javax.transaction.Status;
import javax.transaction.Synchronization;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.testutil.TestTools;

public class Test_044_TransactionsSynchronization {

	@BeforeClass
	public static void setUp() {
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
	}

	@Before
	public void before() {
		TestTools.dropInstances(TestClass.class);
	}

	private static class SyncCommit implements Synchronization {

		private int before = 0; //use int to catch possible double-calls 
		private int after = 0; 
		
		@Override
		public void afterCompletion(int arg0) {
			after++;
			assertEquals(before, after);
			assertEquals(Status.STATUS_COMMITTED, arg0);
		}

		@Override
		public void beforeCompletion() {
			assertEquals(before, after);
			before++;
		}
		
	}
	
	private static class SyncRollback implements Synchronization {

		private int after = 0; //use int to catch possible double-calls
		
		@Override
		public void afterCompletion(int arg0) {
			after++;
			assertEquals(Status.STATUS_ROLLEDBACK, arg0);
		}

		@Override
		public void beforeCompletion() {
			fail();
		}
		
	}
	
	@Test
	public void testSync() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		SyncCommit sC = new SyncCommit();
		SyncRollback sR = new SyncRollback();
		
		//check get/set
		SyncCommit temp = new SyncCommit();
		pm.currentTransaction().setSynchronization(temp);
		pm.currentTransaction().setSynchronization(sC);
		assertTrue(pm.currentTransaction().getSynchronization() == sC);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		assertEquals(1, sC.before);
		assertEquals(1, sC.after);

		pm.currentTransaction().setSynchronization(sR);
		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();

		assertEquals(1, sR.after);

		//set null
		pm.currentTransaction().setSynchronization(null);
		
		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();
		pm.currentTransaction().commit();
		
		//check nothing has changed
		assertEquals(1, sC.before);
		assertEquals(1, sC.after);
		assertEquals(1, sR.after);
		
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

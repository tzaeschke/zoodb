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
package org.zoodb.test.jdo;

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
		assertSame(pm.currentTransaction().getSynchronization(), sC);
		
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

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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.test.testutil.TestTools;
import org.zoodb.test.testutil.rmi.RmiTaskRunner;
import org.zoodb.test.testutil.rmi.RmiTestTask;

public class Test_020_Session {
	
	private static final String DB_NAME = "TestDb";
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb(DB_NAME);
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb(DB_NAME);
	}
	
	@Test
	public void testCreateAndCloseSession() {
		ZooJdoProperties props = new ZooJdoProperties(DB_NAME);
		PersistenceManagerFactory pmf1 = 
			JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm11 = pmf1.getPersistenceManager();

		assertFalse(pm11.isClosed());
		pm11.close();
		assertTrue(pm11.isClosed());
	
		pmf1.close();
		
		try {
			pmf1.getPersistenceManager();
			fail();
		} catch (JDOUserException e) {
			//good, it's closed!
		}
		
		try {
			pmf1.setConnectionURL("xyz");
			fail();
		} catch (JDOUserException e) {
			//good, there are still open session!
		}
	}
	
	@Test
	public void testCreateAndClosePMF_Issue48() {
		ZooJdoProperties props = new ZooJdoProperties(DB_NAME);
		PersistenceManagerFactory pmf = 
			JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm1 = pmf.getPersistenceManager();
		PersistenceManager pm2 = pmf.getPersistenceManager();

		pm1.currentTransaction().begin();
		try {
			pmf.close();
			fail();
		} catch (JDOUserException e) {
			//good, it's still active!
		}

		//a failed close should close none of the associated transactions (JDO 3.0 11.4)
		assertFalse(pm1.isClosed());
		assertFalse(pm2.isClosed());
		
		pm1.currentTransaction().commit();
		pmf.close();  //should close the pm
		
		assertTrue(pm1.isClosed());
		assertTrue(pm2.isClosed());
	}
	
	@Test
	public void testIgnoreCacheSettings() {
		ZooJdoProperties props = new ZooJdoProperties(DB_NAME);
		PersistenceManagerFactory pmf = 
			JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm = pmf.getPersistenceManager();
		assertFalse(pmf.getIgnoreCache());
		assertFalse(pm.getIgnoreCache());
		
		pm.setIgnoreCache(true);
		assertFalse(pmf.getIgnoreCache());
		assertTrue(pm.getIgnoreCache());
		pm.close();
		
		
		pmf.setIgnoreCache(true);
		assertTrue(pmf.getIgnoreCache());
		pm = pmf.getPersistenceManager();
		assertTrue(pm.getIgnoreCache());
		pm.close();
		pmf.close();
		
		
		props.setIgnoreCache(true);
		pmf = JDOHelper.getPersistenceManagerFactory(props);
		pm = pmf.getPersistenceManager();
		assertTrue(pmf.getIgnoreCache());
		assertTrue(pm.getIgnoreCache());

		pm.close();
		pmf.close();
	}
	
	/**
	 * Only one process should be allowed to connect to a database.
	 */
	@Test
	public void testDualProcessAccessFail() {
		ZooJdoProperties props = new ZooJdoProperties(DB_NAME);
		PersistenceManagerFactory pmf1 = 
			JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm11 = pmf1.getPersistenceManager();

		try {
			RmiTaskRunner.executeTask(new TestDualProcessFail());
			fail();
		} catch (JDOUserException e) {
			//good
		}
		
		pm11.close();
		pmf1.close();
	}
	
	
	static class TestDualProcessFail implements RmiTestTask {

		private static final long serialVersionUID = 1L;

		@Override
		public void test() {
			ZooJdoProperties props = new ZooJdoProperties(DB_NAME);
			PersistenceManagerFactory pmf1 = 
				JDOHelper.getPersistenceManagerFactory(props);

			// ************************************************
			// Currently we do not support multiple session.
			// ************************************************
			pmf1.getPersistenceManager();
			fail();
		}
		
	}
}

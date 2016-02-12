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
import org.zoodb.test.testutil.RmiTaskLauncher;
import org.zoodb.test.testutil.RmiTestTask;
import org.zoodb.test.testutil.TestProcess;
import org.zoodb.test.testutil.TestTools;

public class Test_020_Session {
	
	private static final String DB_NAME = "TestDb";

	private static TestProcess rmi = null;
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb(DB_NAME);
	}
	
	@AfterClass
	public static void tearDown() {
		if (rmi != null) {
			rmi.stop();
		}
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
		rmi = TestProcess.launchRMI();

		ZooJdoProperties props = new ZooJdoProperties(DB_NAME);
		PersistenceManagerFactory pmf1 = 
			JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm11 = pmf1.getPersistenceManager();

		try {
			RmiTaskLauncher.runTest(new TestDualProcessFail());
		} catch (JDOUserException e) {
			//good
		}
		
		//just in case:
		rmi.stop();
		rmi = null;
		
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

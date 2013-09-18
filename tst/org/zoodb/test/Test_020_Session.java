/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
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
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.test.testutil.TestTools;

public class Test_020_Session {
	
	private static final String DB_NAME = "TestDb";
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb(DB_NAME);
	}

	@Test
	public void testCreateAndCloseSession() {
		ZooJdoProperties props = new ZooJdoProperties(DB_NAME);
		PersistenceManagerFactory pmf1 = 
			JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm11 = pmf1.getPersistenceManager();

		PersistenceManagerFactory pmf2 = 
			JDOHelper.getPersistenceManagerFactory(props);

		try {
			// ************************************************
			// Currently we do not support multiple session.
			// ************************************************
			pmf2.getPersistenceManager();
			fail();
		} catch (Exception e) {
			//good
		}


//		PersistenceManager pm21 = pmf2.getPersistenceManager();
//		
//		//should have returned different pm's
//		assertFalse(pm21 == pm11);
//
//		PersistenceManager pm12 = pmf1.getPersistenceManager();
//		//should never return same pm (JDO spec 2.2/11.2)
//		assertTrue(pm12 != pm11);
//
//		try {
//			pmf1.close();
//			fail();
//		} catch (JDOUserException e) {
//			//good, there are still open session!
//		}
//		
		assertFalse(pm11.isClosed());
//		assertFalse(pm12.isClosed());
		pm11.close();
//		pm12.close();
		assertTrue(pm11.isClosed());
//		assertTrue(pm12.isClosed());
//	
//		assertFalse(pm21.isClosed());
//		pm21.close();
//		assertTrue(pm21.isClosed());
//
		pmf1.close();
//		pmf2.close();
		
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
	 * But should a single process be allowed to create multiple PMFs?
	 */
	@Test
	public void testDualSession() {
		//TODO
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb(DB_NAME);
	}
}

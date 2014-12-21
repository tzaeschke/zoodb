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
package org.zoodb.test.jdo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNull;

import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import javax.jdo.JDOHelper;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.JDOUserException;
import javax.jdo.ObjectState;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.test.testutil.TestTools;

public class Test_041_TransactionsCommit {

	private static final String DB_NAME = "TestDb";
	
	private PersistenceManager pm;
	private PersistenceManagerFactory pmf;
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb(DB_NAME);
		TestTools.defineSchema(TestClass.class);
	}

	@Test
	public void testNewDelete() {
		Properties props = new ZooJdoProperties(DB_NAME);
		pmf = JDOHelper.getPersistenceManagerFactory(props);
		pm = pmf.getPersistenceManager();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		pm.makePersistent(tc);
		Object oid = pm.getObjectId(tc);
		pm.deletePersistent(tc);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		try {
			assertNull(pm.getObjectById(oid));
			fail();
		} catch (JDOObjectNotFoundException e) {
			//good
		}

		try {
			pm.deletePersistent(tc);
			fail();
		} catch (JDOUserException e) {
			//good
		}
		pm.currentTransaction().commit();
		pm.close();
	}

	@Test
	public void testDoubleDelete() {
		Properties props = new ZooJdoProperties(DB_NAME);
		pmf = JDOHelper.getPersistenceManagerFactory(props);
		pm = pmf.getPersistenceManager();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		pm.makePersistent(tc);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		pm.deletePersistent(tc);
		assertEquals(ObjectState.PERSISTENT_DELETED, JDOHelper.getObjectState(tc));
		try {
			pm.deletePersistent(tc);
			fail();
		} catch (JDOUserException e) {
			//good
		}

		assertEquals(ObjectState.PERSISTENT_DELETED, JDOHelper.getObjectState(tc));
		pm.refresh(tc);
		assertEquals(ObjectState.PERSISTENT_DELETED, JDOHelper.getObjectState(tc));

		pm.currentTransaction().commit();
		pm.close();
	}
	
	@Test
	public void testNewDeleteAllObj() {
		Properties props = new ZooJdoProperties(DB_NAME);
		pmf = JDOHelper.getPersistenceManagerFactory(props);
		pm = pmf.getPersistenceManager();
		pm.currentTransaction().begin();
		
		TestClass tc1 = new TestClass();
		TestClass tc2 = new TestClass();
		pm.makePersistent(tc1);
		pm.makePersistent(tc2);
		Object oid1 = pm.getObjectId(tc1);
		Object oid2 = pm.getObjectId(tc2);
		pm.deletePersistentAll(tc1, tc2);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		try {
			assertNull(pm.getObjectById(oid1));
			fail();
		} catch (JDOObjectNotFoundException e) {
			//good
		}

		try {
			assertNull(pm.getObjectById(oid2));
			fail();
		} catch (JDOObjectNotFoundException e) {
			//good
		}

		try {
			pm.deletePersistentAll(tc1, tc2);
			fail();
		} catch (JDOUserException e) {
			//good
		}
		pm.currentTransaction().commit();
		pm.close();
	}

	@Test
	public void testDoubleDeleteAllObj() {
		Properties props = new ZooJdoProperties(DB_NAME);
		pmf = JDOHelper.getPersistenceManagerFactory(props);
		pm = pmf.getPersistenceManager();
		pm.currentTransaction().begin();
		
		TestClass tc1 = new TestClass();
		TestClass tc2 = new TestClass();
		pm.makePersistent(tc1);
		pm.makePersistent(tc2);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		pm.deletePersistentAll(tc1, tc2);
		try {
			pm.deletePersistent(tc1);
			fail();
		} catch (JDOUserException e) {
			//good
		}
		
		try {
			pm.deletePersistentAll(tc1, tc2);
			fail();
		} catch (JDOUserException e) {
			//good
		}
		
		pm.refreshAll(tc1, tc2);
		assertEquals(ObjectState.PERSISTENT_DELETED, JDOHelper.getObjectState(tc1));
		assertEquals(ObjectState.PERSISTENT_DELETED, JDOHelper.getObjectState(tc2));

		pm.refreshAll();
		assertEquals(ObjectState.PERSISTENT_DELETED, JDOHelper.getObjectState(tc1));
		assertEquals(ObjectState.PERSISTENT_DELETED, JDOHelper.getObjectState(tc2));

		pm.refresh(tc1);
		assertEquals(ObjectState.PERSISTENT_DELETED, JDOHelper.getObjectState(tc1));

		pm.currentTransaction().commit();
		pm.close();
	}
	
	@Test
	public void testNonPersistentObject() {
		Properties props = new ZooJdoProperties(DB_NAME);
		pmf = JDOHelper.getPersistenceManagerFactory(props);
		pm = pmf.getPersistenceManager();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		
		try {
			pm.deletePersistent(tc);
			fail();
		} catch (JDOUserException e) {
			//good
		}

		pm.refresh(tc);
		assertEquals(ObjectState.TRANSIENT, JDOHelper.getObjectState(tc));

		pm.currentTransaction().commit();
		pm.close();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testTransactionBoundaries() {
		Properties props = new ZooJdoProperties(DB_NAME);
		pmf = JDOHelper.getPersistenceManagerFactory(props);
		pm = pmf.getPersistenceManager();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		tc.setInt(5);
		pm.makePersistent(tc);
		TestClass tcD = new TestClass();
		tcD.setInt(55);
		pm.makePersistent(tcD);
		
		pm.currentTransaction().commit();

		//delete one object
		pm.currentTransaction().begin();
		pm.deletePersistent(tcD);
		pm.currentTransaction().commit();
		pm.close();

	
		// test with closed TX
		assertGetIntFails(tc);
		assertGetIntFails(tcD);
		
		pm = pmf.getPersistenceManager();
		pm.currentTransaction().begin();
		
		// check access with wrong pm
		assertGetIntFails(tc);
		assertGetIntFails(tcD);

		//check deletion again
		try {
			pm.deletePersistent(tc);
			fail();
		} catch (JDOUserException e) {
			//good
		}
		pm.currentTransaction().commit();
		pm.close();
		
		
		//test query
		pm = pmf.getPersistenceManager();
		pm.currentTransaction().begin();

		Query q = pm.newQuery(TestClass.class, "_int >= 5");
		Collection<TestClass> c = (Collection<TestClass>) q.execute();
		Iterator<TestClass> it = c.iterator();
		assertEquals(5, it.next().getInt());
		assertFalse(it.hasNext());
		
		//close
		pm.currentTransaction().commit();
		pm.close();
		pmf.close();
	}
	
	private void assertGetIntFails(TestClass tc) {
		try {
			tc.getInt();
			fail();
		} catch (JDOUserException e) {
			//good
		}
	}
	
	@After
	public void afterTest() {
		if (pm != null && !pm.isClosed()) {
			if (pm.currentTransaction().isActive()) {
				pm.currentTransaction().rollback();
			}
			pm.close();
		}
		pm = null;
		if (pmf != null && !pmf.isClosed()) {
			pmf.close();
		}
		pmf = null;
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb(DB_NAME);
	}
}

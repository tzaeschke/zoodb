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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;

import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.JDOHelper;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.schema.ZooClass;
import org.zoodb.schema.ZooHandle;
import org.zoodb.test.api.TestSuper;
import org.zoodb.test.testutil.TestTools;
import org.zoodb.tools.DBStatistics;
import org.zoodb.tools.DBStatistics.STATS;

public class Test_022_MultiSessionSchema {
	
	@Before
	public void setUp() {
		TestTools.removeDb();
		TestTools.createDb();
	}
	
	@After
	public void tearDown() {
		TestTools.removeDb();
	}
	

	@Test
	public void testCommitFailSchema() {
		System.err.println("Implement me!");
		fail();
//		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
//		PersistenceManagerFactory pmf = 
//			JDOHelper.getPersistenceManagerFactory(props);
//		PersistenceManager pm1 = pmf.getPersistenceManager();
//		PersistenceManager pm2 = pmf.getPersistenceManager();
//		pm1.currentTransaction().begin();
//		pm2.currentTransaction().begin();
//
//		ZooSchema s1 = ZooJdoHelper.schema(pm1);
//		ZooSchema s2 = ZooJdoHelper.schema(pm2);
//		
//		//new TestSuper(1, 11, null);
//		ZooClass t11 = s1.defineEmptyClass("Class11");
//		ZooClass t12 = s1.defineEmptyClass("Class22");
//		ZooClass t13 = s1.defineEmptyClass("Class33");
//		ZooClass t14 = s1.defineEmptyClass("Class44");
//		ZooClass t15 = s1.defineEmptyClass("Class55");
//		
//		//TODO test normal schema updates (new attr) and naming conflicts separately!
//		//E.g. create class with name before cponciurring class is committed
//		
//		long oid1 = t11.getOid();
//		long oid2 = t12.getOid();
//		long oid3 = t13.getOid();
//		long oid4 = t14.getOid();
//		long oid5 = t15.getOid();
//		
//		pm1.currentTransaction().commit();
//		pm1.currentTransaction().begin();
//		
//		//concurrent modification
//		ZooHandle t21 = s2.getHandle(oid1);
//		ZooHandle t22 = s2.getHandle(oid2);
//		ZooHandle t23 = s2.getHandle(oid3);
//		ZooHandle t24 = s2.getHandle(oid4);
//		ZooHandle t25 = s2.getHandle(oid5);
//
//		//modified by both: t1, t3
//		//modified by 1: t4
//		//modified by 2: t5
//		t21.setValue("_id", 21L);
//		t23.setValue("_id", 23L);
//		t25.setValue("_id", 25L);
//		
//		t11.setValue("_id", 11L);
//		t13.setValue("_id", 13L);
//		t14.setValue("_id", 14L);
//		
//		pm2.currentTransaction().commit();
//		pm2.currentTransaction().begin();
//
//		DBStatistics stats = ZooJdoHelper.getStatistics(pm2);
//		System.out.println("s1_oids="+ stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));
//		System.out.println("s1_tx="+ stats.getStat(STATS.TX_MGR_BUFFERED_TX_CNT));
//		//5?
//		assertEquals(5, stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));
//		
//		try {
//			pm1.currentTransaction().commit();
//			fail();
//		} catch (JDOFatalDataStoreException e) {
//			//good!
//			HashSet<Object> failedOids = new HashSet<>();
//			for (Throwable t: e.getNestedExceptions()) {
//				Object f = ((JDOFatalDataStoreException)t).getFailedObject();
//				failedOids.add(JDOHelper.getObjectId(f));
//			}
//			assertEquals(2, failedOids.size());
//			assertTrue(failedOids.contains(oid1));
//			assertTrue(failedOids.contains(oid3));
//		}
//
//		//5?
//		assertEquals(5, stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));
//
//		pm1.currentTransaction().begin();
//
//		t11.setValue("_id", t11.getAttrLong("_id") + 1000L);
//		t13.setValue("_id", t13.getAttrLong("_id") + 1000L);
//		t14.setValue("_id", 14L);
//		pm1.currentTransaction().commit();
//		pm1.currentTransaction().begin();
//
//		assertEquals(21+1000, t21.getAttrLong("_id"));
//		assertEquals(22, t22.getAttrLong("_id"));
//		assertEquals(23+1000, t23.getAttrLong("_id"));
//		assertEquals(14, t24.getAttrLong("_id"));
//		assertEquals(25, t25.getAttrLong("_id"));
//		
//
//		pm1.currentTransaction().rollback();
//		pm2.currentTransaction().rollback();
//		
//		//5?
//		assertEquals(5, stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));
//
//		pm1.close();
//		pm2.close();
//		pmf.close();
	}
	
	@Test
	public void testCommitFailWithDeleteSchema() {
		System.err.println("Implement me!");
		fail();
//		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
//		PersistenceManagerFactory pmf = 
//			JDOHelper.getPersistenceManagerFactory(props);
//		TestTools.defineSchema(TestSuper.class);
//		PersistenceManager pm1 = pmf.getPersistenceManager();
//		PersistenceManager pm2 = pmf.getPersistenceManager();
//		pm1.currentTransaction().begin();
//		pm2.currentTransaction().begin();
//
//		ZooSchema s1 = ZooJdoHelper.schema(pm1);
//		ZooSchema s2 = ZooJdoHelper.schema(pm2);
//		
//		//new TestSuper(1, 11, null);
//		ZooClass t11 = s1.defineEmptyClass("Class11");
//		ZooClass t12 = s1.defineEmptyClass("Class22");
//		ZooClass t13 = s1.defineEmptyClass("Class33");
//		ZooClass t14 = s1.defineEmptyClass("Class44");
//		ZooClass t15 = s1.defineEmptyClass("Class55");
//		
//		long oid1 = t11.getOid();
//		long oid2 = t12.getOid();
//		long oid3 = t13.getOid();
//		long oid4 = t14.getOid();
//		long oid5 = t15.getOid();
//		
//		pm1.currentTransaction().commit();
//		pm1.currentTransaction().begin();
//		
//		//concurrent modification
//		ZooHandle t21 = s2.getHandle(oid1);
//		ZooHandle t22 = s2.getHandle(oid2);
//		ZooHandle t23 = s2.getHandle(oid3);
//		ZooHandle t24 = s2.getHandle(oid4);
//		ZooHandle t25 = s2.getHandle(oid5);
//
//		//deleted by both: t1
//		//del/mod t2, t3
//		//deleted by 1: t4
//		//deleted by 2: t5
//		t21.remove();
//		t22.remove();
//		t23.setValue("_id", 23L);
//		t25.remove();
//		
//		t11.remove();
//		t12.setValue("_id", 12L);
//		t13.remove();
//		t14.remove();
//		
//		pm2.currentTransaction().commit();
//		pm2.currentTransaction().begin();
//
//		try {
//			pm1.currentTransaction().commit();
//			fail();
//		} catch (JDOFatalDataStoreException e) {
//			//good!
//			HashSet<Object> failedOids = new HashSet<>();
//			for (Throwable t: e.getNestedExceptions()) {
//				Object f = ((JDOFatalDataStoreException)t).getFailedObject();
//				failedOids.add(JDOHelper.getObjectId(f));
//			}
//			assertEquals(3, failedOids.size());
//			assertTrue(failedOids.contains(oid1));  //double delete
//			assertTrue(failedOids.contains(oid2));  //update deleted obj.
//			assertTrue(failedOids.contains(oid3));  //delete updated obj.
//		}
//		
//		pm1.currentTransaction().begin();
//
//		try {
//			t11.setValue("_id", 12345L);
//			fail();
//		} catch (JDOObjectNotFoundException e) {
//			//good
//		}
//		t13.setValue("_id", t13.getAttrLong("_id") + 1000);
//		t14.setValue("_id", 14L);
//		pm1.currentTransaction().commit();
//		pm1.currentTransaction().begin();
//
//		assertTrue(JDOHelper.isDeleted(t21));
//		assertTrue(JDOHelper.isDeleted(t22));
//		assertEquals(23+1000, t23.getAttrLong("_id"));
//		assertEquals(14, t24.getAttrLong("_id"));
//		assertTrue(JDOHelper.isDeleted(t25));
//		
//
//		pm1.currentTransaction().rollback();
//		pm2.currentTransaction().rollback();
//		
//		pm1.close();
//		pm2.close();
//		pmf.close();
	}

	@Test
	public void testSchemaDrop() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf = 
			JDOHelper.getPersistenceManagerFactory(props);
		TestTools.defineSchema(TestSuper.class);
		TestTools.defineSchema(TestClass.class);
		PersistenceManager pm1 = pmf.getPersistenceManager();
		PersistenceManager pm2 = pmf.getPersistenceManager();
		pm1.currentTransaction().begin();
		pm2.currentTransaction().begin();

		TestSuper t11 = new TestSuper(1, 11, null);
		TestSuper t12 = new TestSuper(2, 22, null);
		TestSuper t13 = new TestSuper(3, 33, null);
		TestSuper t14 = new TestSuper(4, 44, null);
		TestSuper t15 = new TestSuper(5, 55, null);
		
		pm1.makePersistent(t11);
		pm1.makePersistent(t12);
		pm1.makePersistent(t13);
		pm1.makePersistent(t14);
		pm1.makePersistent(t15);
		
		Object oid1 = pm1.getObjectId(t11);
		Object oid2 = pm1.getObjectId(t12);
		Object oid3 = pm1.getObjectId(t13);
		Object oid4 = pm1.getObjectId(t14);
		Object oid5 = pm1.getObjectId(t15);
		
		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();
		
		//concurrent modification
		TestSuper t21 = (TestSuper) pm2.getObjectById(oid1);
		TestSuper t22 = (TestSuper) pm2.getObjectById(oid2);
		TestSuper t23 = (TestSuper) pm2.getObjectById(oid3);
		TestSuper t24 = (TestSuper) pm2.getObjectById(oid4);
		TestSuper t25 = (TestSuper) pm2.getObjectById(oid5);

		//deleted by both: t1
		//del/mod t2, t3
		//deleted by 1: t4
		//deleted by 2: t5
		pm2.deletePersistent(t21);
		pm2.deletePersistent(t22);
		t23.setId(23);
		pm2.deletePersistent(t25);

		ZooClass cls1 = ZooJdoHelper.schema(pm1).getClass(TestSuper.class);
		cls1.dropInstances();
//		pm1.deletePersistent(t11);
//		t12.setId(12);
//		pm1.deletePersistent(t13);
//		pm1.deletePersistent(t14);
		
		pm2.currentTransaction().commit();
		pm2.currentTransaction().begin();

		try {
			pm1.currentTransaction().commit();
			fail();
		} catch (JDOFatalDataStoreException e) {
			//good!
			HashSet<Object> failedOids = new HashSet<>();
			for (Throwable t: e.getNestedExceptions()) {
				Object f = ((JDOFatalDataStoreException)t).getFailedObject();
				failedOids.add(JDOHelper.getObjectId(f));
			}
			assertEquals(3, failedOids.size());
			assertTrue(failedOids.contains(oid1));  //double delete
			assertTrue(failedOids.contains(oid2));  //update deleted obj.
			assertTrue(failedOids.contains(oid3));  //delete updated obj.
		}
		
		pm1.currentTransaction().begin();

		try {
			t11.setId(12345);
			fail();
		} catch (JDOObjectNotFoundException e) {
			//good
		}
		cls1.remove();
//		t13.setId(t13.getId() + 1000);
//		t14.setId(14);
//		pm1.currentTransaction().commit();
//		pm1.currentTransaction().begin();

		assertTrue(JDOHelper.isDeleted(t21));
		assertTrue(JDOHelper.isDeleted(t22));
		assertEquals(23+1000, t23.getId());
		assertEquals(14, t24.getId());
		assertTrue(JDOHelper.isDeleted(t25));
		

		pm1.currentTransaction().rollback();
		pm2.currentTransaction().rollback();
		
		pm1.close();
		pm2.close();
		pmf.close();
	}
	
	
	@Test
	public void testCheckConsistency() {
		System.err.println("Implement me!");
		fail();
		//TODO insert this into above tests...
	}
}

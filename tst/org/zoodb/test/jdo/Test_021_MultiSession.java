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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;

import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.JDOHelper;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.schema.ZooClass;
import org.zoodb.schema.ZooHandle;
import org.zoodb.schema.ZooSchema;
import org.zoodb.test.api.TestSuper;
import org.zoodb.test.testutil.TestTools;
import org.zoodb.tools.DBStatistics;
import org.zoodb.tools.DBStatistics.STATS;

public class Test_021_MultiSession {
	
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
	public void testCreateAndCloseSession() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf1 = 
			JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm11 = pmf1.getPersistenceManager();

		PersistenceManagerFactory pmf2 = 
			JDOHelper.getPersistenceManagerFactory(props);

		PersistenceManager pm21 = pmf2.getPersistenceManager();
		
		//should have returned different pm's
		assertFalse(pm21 == pm11);

		PersistenceManager pm12 = pmf1.getPersistenceManager();
		//should never return same pm (JDO spec 2.2/11.2)
		assertTrue(pm12 != pm11);

		try {
			pmf1.close();
			fail();
		} catch (JDOUserException e) {
			//good, there are still open session!
		}
		
		assertFalse(pm11.isClosed());
		assertFalse(pm12.isClosed());
		pm11.close();
		pm12.close();
		assertTrue(pm11.isClosed());
		assertTrue(pm12.isClosed());
	
		assertFalse(pm21.isClosed());
		pm21.close();
		assertTrue(pm21.isClosed());

		pmf1.close();
		pmf2.close();
		
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
	public void testCommitFail() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf = 
			JDOHelper.getPersistenceManagerFactory(props);
		TestTools.defineSchema(TestSuper.class);
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

		//modified by both: t1, t3
		//modified by 1: t4
		//modified by 2: t5
		t21.setId(21);
		t23.setId(23);
		t25.setId(25);
		
		t11.setId(11);
		t13.setId(13);
		t14.setId(14);
		
		pm2.currentTransaction().commit();
		pm2.currentTransaction().begin();

		DBStatistics stats = ZooJdoHelper.getStatistics(pm2);
		System.out.println("s1_oids="+ stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));
		System.out.println("s1_tx="+ stats.getStat(STATS.TX_MGR_BUFFERED_TX_CNT));
		//5?
		assertEquals(5, stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));
		
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
			assertEquals(2, failedOids.size());
			assertTrue(failedOids.contains(oid1));
			assertTrue(failedOids.contains(oid3));
		}

		//5?
		assertEquals(5, stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));

		pm1.currentTransaction().begin();

		t11.setId(t11.getId() + 1000);
		t13.setId(t13.getId() + 1000);
		t14.setId(14);
		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();

		assertEquals(21+1000, t21.getId());
		assertEquals(22, t22.getId());
		assertEquals(23+1000, t23.getId());
		assertEquals(14, t24.getId());
		assertEquals(25, t25.getId());
		

		pm1.currentTransaction().rollback();
		pm2.currentTransaction().rollback();
		
		//5?
		assertEquals(5, stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));

		pm1.close();
		pm2.close();
		pmf.close();
	}
	
	@Test
	public void testCommitFailWithDelete() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf = 
			JDOHelper.getPersistenceManagerFactory(props);
		TestTools.defineSchema(TestSuper.class);
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
		
		pm1.deletePersistent(t11);
		t12.setId(12);
		pm1.deletePersistent(t13);
		pm1.deletePersistent(t14);
		
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
		t13.setId(t13.getId() + 1000);
		t14.setId(14);
		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();

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
	public void testCommitFail_ReducedOverlap() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf = 
			JDOHelper.getPersistenceManagerFactory(props);
		TestTools.defineSchema(TestSuper.class);
		PersistenceManager pm1 = pmf.getPersistenceManager();
		pm1.currentTransaction().begin();

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
		PersistenceManager pm2 = pmf.getPersistenceManager();
		pm2.currentTransaction().begin();
		TestSuper t21 = (TestSuper) pm2.getObjectById(oid1);
		TestSuper t22 = (TestSuper) pm2.getObjectById(oid2);
		TestSuper t23 = (TestSuper) pm2.getObjectById(oid3);
		TestSuper t24 = (TestSuper) pm2.getObjectById(oid4);
		TestSuper t25 = (TestSuper) pm2.getObjectById(oid5);

		//modified by both: t1, t3
		//modified by 1: t4
		//modified by 2: t5
		t21.setId(21);
		t23.setId(23);
		t25.setId(25);
		
		t11.setId(11);
		t13.setId(13);
		t14.setId(14);
		
		pm2.currentTransaction().commit();
		pm2.currentTransaction().begin();

		DBStatistics stats = ZooJdoHelper.getStatistics(pm2);
		System.out.println("s1_oids="+ stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));
		System.out.println("s1_tx="+ stats.getStat(STATS.TX_MGR_BUFFERED_TX_CNT));
		assertEquals(3, stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));
		
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
			assertEquals(2, failedOids.size());
			assertTrue(failedOids.contains(oid1));
			assertTrue(failedOids.contains(oid3));
		}

		assertEquals(3, stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));

		pm1.currentTransaction().begin();

		t11.setId(t11.getId() + 1000);
		t13.setId(t13.getId() + 1000);
		t14.setId(14);
		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();

		assertEquals(21+1000, t21.getId());
		assertEquals(22, t22.getId());
		assertEquals(23+1000, t23.getId());
		assertEquals(14, t24.getId());
		assertEquals(25, t25.getId());
		

		pm1.currentTransaction().rollback();
		pm2.currentTransaction().rollback();
		
		//5?
		assertEquals(4, stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));

		pm1.close();
		pm2.close();
		pmf.close();
	}
	
	@Test
	public void testCommitFailWithDelete_ReducedOverlap() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf = 
			JDOHelper.getPersistenceManagerFactory(props);
		TestTools.defineSchema(TestSuper.class);
		PersistenceManager pm1 = pmf.getPersistenceManager();
		pm1.currentTransaction().begin();

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
		PersistenceManager pm2 = pmf.getPersistenceManager();
		pm2.currentTransaction().begin();
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
		
		pm1.deletePersistent(t11);
		t12.setId(12);
		pm1.deletePersistent(t13);
		pm1.deletePersistent(t14);
		
		pm2.currentTransaction().commit();
		pm2.currentTransaction().begin();

		DBStatistics stats = ZooJdoHelper.getStatistics(pm2);
		System.out.println("s1_oids="+ stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));
		System.out.println("s1_tx="+ stats.getStat(STATS.TX_MGR_BUFFERED_TX_CNT));
		
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
		t13.setId(t13.getId() + 1000);
		t14.setId(14);
		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();

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
	public void testCommitFailGenericObjects() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf = 
			JDOHelper.getPersistenceManagerFactory(props);
		TestTools.defineSchema(TestSuper.class);
		PersistenceManager pm1 = pmf.getPersistenceManager();
		PersistenceManager pm2 = pmf.getPersistenceManager();
		pm1.currentTransaction().begin();
		pm2.currentTransaction().begin();

		ZooSchema s1 = ZooJdoHelper.schema(pm1);
		ZooClass c1 = s1.getClass(TestSuper.class.getName());
		ZooSchema s2 = ZooJdoHelper.schema(pm2);
		
		//new TestSuper(1, 11, null);
		ZooHandle t11 = c1.newInstance();
		t11.setValue("_time", 1L);
		t11.setValue("_id", 11L);
		ZooHandle t12 = c1.newInstance();
		t12.setValue("_time", 2L);
		t12.setValue("_id", 22L);
		ZooHandle t13 = c1.newInstance();
		t13.setValue("_time", 3L);
		t13.setValue("_id", 33L);
		ZooHandle t14 = c1.newInstance();
		t14.setValue("_time", 4L);
		t14.setValue("_id", 44L);
		ZooHandle t15 = c1.newInstance();
		t15.setValue("_time", 5L);
		t15.setValue("_id", 55L);
		
		long oid1 = t11.getOid();
		long oid2 = t12.getOid();
		long oid3 = t13.getOid();
		long oid4 = t14.getOid();
		long oid5 = t15.getOid();
		
		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();
		
		//concurrent modification
		ZooHandle t21 = s2.getHandle(oid1);
		ZooHandle t22 = s2.getHandle(oid2);
		ZooHandle t23 = s2.getHandle(oid3);
		ZooHandle t24 = s2.getHandle(oid4);
		ZooHandle t25 = s2.getHandle(oid5);

		//modified by both: t1, t3
		//modified by 1: t4
		//modified by 2: t5
		t21.setValue("_id", 21L);
		t23.setValue("_id", 23L);
		t25.setValue("_id", 25L);
		
		t11.setValue("_id", 11L);
		t13.setValue("_id", 13L);
		t14.setValue("_id", 14L);
		
		pm2.currentTransaction().commit();
		pm2.currentTransaction().begin();

		DBStatistics stats = ZooJdoHelper.getStatistics(pm2);
		System.out.println("s1_oids="+ stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));
		System.out.println("s1_tx="+ stats.getStat(STATS.TX_MGR_BUFFERED_TX_CNT));
		//5?
		assertEquals(5, stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));
		
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
			assertEquals(2, failedOids.size());
			assertTrue(failedOids.contains(oid1));
			assertTrue(failedOids.contains(oid3));
		}

		//5?
		assertEquals(5, stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));

		pm1.currentTransaction().begin();

		t11.setValue("_id", t11.getAttrLong("_id") + 1000L);
		t13.setValue("_id", t13.getAttrLong("_id") + 1000L);
		t14.setValue("_id", 14L);
		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();

		assertEquals(21+1000, t21.getAttrLong("_id"));
		assertEquals(22, t22.getAttrLong("_id"));
		assertEquals(23+1000, t23.getAttrLong("_id"));
		assertEquals(14, t24.getAttrLong("_id"));
		assertEquals(25, t25.getAttrLong("_id"));
		

		pm1.currentTransaction().rollback();
		pm2.currentTransaction().rollback();
		
		//5?
		assertEquals(5, stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));

		pm1.close();
		pm2.close();
		pmf.close();
	}
	
	@Test
	public void testCommitFailWithDeleteGenericObjects() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf = 
			JDOHelper.getPersistenceManagerFactory(props);
		TestTools.defineSchema(TestSuper.class);
		PersistenceManager pm1 = pmf.getPersistenceManager();
		PersistenceManager pm2 = pmf.getPersistenceManager();
		pm1.currentTransaction().begin();
		pm2.currentTransaction().begin();

		ZooSchema s1 = ZooJdoHelper.schema(pm1);
		ZooClass c1 = s1.getClass(TestSuper.class.getName());
		ZooSchema s2 = ZooJdoHelper.schema(pm2);
		
		//new TestSuper(1, 11, null);
		ZooHandle t11 = c1.newInstance();
		t11.setValue("_time", 1L);
		t11.setValue("_id", 11L);
		ZooHandle t12 = c1.newInstance();
		t12.setValue("_time", 2L);
		t12.setValue("_id", 22L);
		ZooHandle t13 = c1.newInstance();
		t13.setValue("_time", 3L);
		t13.setValue("_id", 33L);
		ZooHandle t14 = c1.newInstance();
		t14.setValue("_time", 4L);
		t14.setValue("_id", 44L);
		ZooHandle t15 = c1.newInstance();
		t15.setValue("_time", 5L);
		t15.setValue("_id", 55L);
		
		long oid1 = t11.getOid();
		long oid2 = t12.getOid();
		long oid3 = t13.getOid();
		long oid4 = t14.getOid();
		long oid5 = t15.getOid();
		
		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();
		
		//concurrent modification
		ZooHandle t21 = s2.getHandle(oid1);
		ZooHandle t22 = s2.getHandle(oid2);
		ZooHandle t23 = s2.getHandle(oid3);
		ZooHandle t24 = s2.getHandle(oid4);
		ZooHandle t25 = s2.getHandle(oid5);

		//deleted by both: t1
		//del/mod t2, t3
		//deleted by 1: t4
		//deleted by 2: t5
		t21.remove();
		t22.remove();
		t23.setValue("_id", 23L);
		t25.remove();
		
		t11.remove();
		t12.setValue("_id", 12L);
		t13.remove();
		t14.remove();
		
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
			t11.setValue("_id", 12345L);
			fail();
		} catch (JDOObjectNotFoundException e) {
			//good
		}
		t13.setValue("_id", t13.getAttrLong("_id") + 1000);
		t14.setValue("_id", 14L);
		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();

		assertTrue(JDOHelper.isDeleted(t21));
		assertTrue(JDOHelper.isDeleted(t22));
		assertEquals(23+1000, t23.getAttrLong("_id"));
		assertEquals(14, t24.getAttrLong("_id"));
		assertTrue(JDOHelper.isDeleted(t25));
		

		pm1.currentTransaction().rollback();
		pm2.currentTransaction().rollback();
		
		pm1.close();
		pm2.close();
		pmf.close();
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
	public void testOidCollision() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf = 
			JDOHelper.getPersistenceManagerFactory(props);
		TestTools.defineSchema(TestSuper.class);
		PersistenceManager pm1 = pmf.getPersistenceManager();
		PersistenceManager pm2 = pmf.getPersistenceManager();
		pm1.currentTransaction().begin();
		pm2.currentTransaction().begin();

		ZooClass c2 = ZooJdoHelper.schema(pm2).getClass(TestSuper.class);

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
		
		//concurrent modification
		ZooHandle t21 = c2.newInstance((Long) oid1);
		ZooHandle t22 = c2.newInstance((Long) oid2);
		ZooHandle t23 = c2.newInstance((Long) oid3);
		ZooHandle t24 = c2.newInstance((Long) oid4);
		ZooHandle t25 = c2.newInstance((Long) oid5);
//		TestSuper t21 = (TestSuper) pm2.getObjectById(oid1);
//		TestSuper t22 = (TestSuper) pm2.getObjectById(oid2);
//		TestSuper t23 = (TestSuper) pm2.getObjectById(oid3);
//		TestSuper t24 = (TestSuper) pm2.getObjectById(oid4);
//		TestSuper t25 = (TestSuper) pm2.getObjectById(oid5);

		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();
		
		//modified by both: t1, t3
		//modified by 1: t4
		//modified by 2: t5
//		t21.setId(21);
//		t23.setId(23);
//		t25.setId(25);
		t21.setValue("_id", 21L);
		t23.setValue("_id", 23L);
		t25.setValue("_id", 25L);
		
		t11.setId(11);
		t13.setId(13);
		t14.setId(14);
		
		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();

		DBStatistics stats = ZooJdoHelper.getStatistics(pm1);
		System.out.println("s1_oids="+ stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));
		System.out.println("s1_tx="+ stats.getStat(STATS.TX_MGR_BUFFERED_TX_CNT));
		//5?
		assertEquals(5, stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));
		
		try {
			pm2.currentTransaction().commit();
			fail();
		} catch (JDOFatalDataStoreException e) {
			//good!
			HashSet<Object> failedOids = new HashSet<>();
			for (Throwable t: e.getNestedExceptions()) {
				Object f = ((JDOFatalDataStoreException)t).getFailedObject();
				failedOids.add(JDOHelper.getObjectId(f));
			}
			assertEquals(2, failedOids.size());
			assertTrue(failedOids.contains(oid1));
			assertTrue(failedOids.contains(oid3));
		}

		//5?
		assertEquals(5, stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));

		pm2.currentTransaction().begin();

		//TODO revert pm1 and pm2... ?
		t11.setId(t11.getId() + 1000);
		t13.setId(t13.getId() + 1000);
		t14.setId(14);
		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();

		assertEquals(21+1000, t21.getAttrLong("_id"));
		assertEquals(22, t22.getAttrLong("_id"));
		assertEquals(23+1000, t23.getAttrLong("_id"));
		assertEquals(14, t24.getAttrLong("_id"));
		assertEquals(25, t25.getAttrLong("_id"));
		

		pm1.currentTransaction().rollback();
		pm2.currentTransaction().rollback();
		
		//5?
		assertEquals(5, stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));

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

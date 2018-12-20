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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashSet;

import javax.jdo.JDOHelper;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.JDOOptimisticVerificationException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.internal.server.DiskAccessOneFile;
import org.zoodb.internal.util.RWSemaphoreSync;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.schema.ZooClass;
import org.zoodb.schema.ZooHandle;
import org.zoodb.schema.ZooSchema;
import org.zoodb.test.api.TestSuper;
import org.zoodb.test.testutil.TestTools;
import org.zoodb.tools.DBStatistics;
import org.zoodb.tools.DBStatistics.STATS;

public class Test_023_GenericObjects {
	
	private boolean checkDBLock;
	
	@Before
	public void setUp() {
		TestTools.removeDb();
		TestTools.createDb();
		DiskAccessOneFile.allowReadConcurrency(true);
		checkDBLock = RWSemaphoreSync.setCheckDbLock(false); 
	}
	
	@After
	public void tearDown() {
		DiskAccessOneFile.allowReadConcurrency(false);
		RWSemaphoreSync.setCheckDbLock(checkDBLock);
		TestTools.removeDb();
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
		//System.out.println("s1_oids="+ stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));
		//System.out.println("s1_tx="+ stats.getStat(STATS.TX_MGR_BUFFERED_TX_CNT));
		//5?
		assertEquals(5, stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));
		
		try {
			pm1.currentTransaction().commit();
			fail();
		} catch (JDOOptimisticVerificationException e) {
			//good!
			HashSet<Object> failedOids = new HashSet<>();
			for (Throwable t: e.getNestedExceptions()) {
				Object f = ((JDOOptimisticVerificationException)t).getFailedObject();
				failedOids.add(JDOHelper.getObjectId(f));
			}
			assertEquals(Arrays.toString(failedOids.toArray()), 2, failedOids.size());
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
		} catch (JDOOptimisticVerificationException e) {
			//good!
			HashSet<Object> failedOids = new HashSet<>();
			for (Throwable t: e.getNestedExceptions()) {
				Object f = ((JDOOptimisticVerificationException)t).getFailedObject();
				assertTrue(f instanceof ZooHandle);
				failedOids.add(JDOHelper.getObjectId(f));
			}
			assertEquals(Arrays.toString(failedOids.toArray()), 3, failedOids.size());
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
	public void testOidCollisionWithNew() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf = 
			JDOHelper.getPersistenceManagerFactory(props);
		TestTools.defineSchema(TestSuper.class);
		PersistenceManager pm1 = pmf.getPersistenceManager();
		PersistenceManager pm2 = pmf.getPersistenceManager();
		pm1.currentTransaction().begin();
		pm2.currentTransaction().begin();

		ZooClass c2 = ZooJdoHelper.schema(pm2).getClass(TestSuper.class);

		ZooHandle[] t1 = new ZooHandle[5];
		long[] oids1 = new long[5];
		createData(pm1, t1, oids1);
		
		//concurrent modification
		ZooHandle t20 = c2.newInstance((Long) oids1[0]);
		ZooHandle t21 = c2.newInstance((Long) oids1[1]);
		ZooHandle t22 = c2.newInstance((Long) oids1[2]);
		ZooHandle t23 = c2.newInstance();
		ZooHandle t24 = c2.newInstance((Long) oids1[4]);

		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();
		
		//modified by both: t1, t3
		//modified by 1: t4
		//modified by 2: t5
		t20.setValue("_id", 20L);
		t22.setValue("_id", 22L);
		t24.setValue("_id", 24L);
		
		t1[0].setValue("_id", 10L);
		t1[2].setValue("_id", 12L);
		t1[3].setValue("_id", 13L);
		
		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();

		DBStatistics stats = ZooJdoHelper.getStatistics(pm1);
		//5?
		assertEquals(5, stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));
		
		try {
			pm2.checkConsistency();
			fail();
		} catch (JDOOptimisticVerificationException e) {
			//good!
			HashSet<Object> failedOids = new HashSet<>();
			for (Throwable t: e.getNestedExceptions()) {
				Object f = ((JDOOptimisticVerificationException)t).getFailedObject();
				failedOids.add(JDOHelper.getObjectId(f));
			}
			assertEquals(Arrays.toString(failedOids.toArray()), 4, failedOids.size());
			assertTrue(failedOids.contains(oids1[0]));
			assertTrue(failedOids.contains(oids1[1]));
			assertTrue(failedOids.contains(oids1[2]));
			assertTrue(failedOids.contains(oids1[4]));
		}
		
		try {
			pm2.currentTransaction().commit();
			fail();
		} catch (JDOOptimisticVerificationException e) {
			//good!
			HashSet<Object> failedObjects = new HashSet<>();
			for (Throwable t: e.getNestedExceptions()) {
				Object f = ((JDOOptimisticVerificationException)t).getFailedObject();
				failedObjects.add(f);
			}
			assertEquals(Arrays.toString(failedObjects.toArray()), 4, failedObjects.size());
			assertTrue(failedObjects.contains(t20));
			assertTrue(failedObjects.contains(t21));
			assertTrue(failedObjects.contains(t22));
			assertTrue(failedObjects.contains(t24));
		}

		//5?
		assertEquals(5, stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));

		pm2.currentTransaction().begin();

		//TODO revert pm1 and pm2... ?
		t1[0].setValue("_id", t1[0].getAttrLong("_id") + 1000);
		t1[2].setValue("_id", t1[2].getAttrLong("_id") + 1000);
		t1[3].setValue("_id", 13L);
		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();

		//these are new instances, fail+rollback does not affect values
		assertEquals(20, t20.getAttrLong("_id"));
		assertEquals(0, t21.getAttrLong("_id"));
		assertEquals(22, t22.getAttrLong("_id"));
		assertEquals(0, t23.getAttrLong("_id"));
		assertEquals(24, t24.getAttrLong("_id"));
		
		//extra test:
		//t20 should be transient now.
		//try to makePersistent and commit()
		assertFalse(JDOHelper.isPersistent(t20));
		pm2.makePersistent(t20);
		
		pm2.currentTransaction().commit();
		pm2.currentTransaction().begin();
		
		assertTrue(JDOHelper.isPersistent(t20));
		pm2.makeTransient(t20);
		assertFalse(JDOHelper.isPersistent(t20));
		
		
		pm1.currentTransaction().rollback();
		pm2.currentTransaction().rollback();
		
		//5?
		assertEquals(5, stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));

		pm1.close();
		pm2.close();
		pmf.close();
	}
	
	@Test
	public void testOidCollisionWithSetOid() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf = 
			JDOHelper.getPersistenceManagerFactory(props);
		TestTools.defineSchema(TestSuper.class);
		PersistenceManager pm1 = pmf.getPersistenceManager();
		PersistenceManager pm2 = pmf.getPersistenceManager();
		pm1.currentTransaction().begin();
		pm2.currentTransaction().begin();

//		ZooSchema s1 = ZooJdoHelper.schema(pm1);
		ZooSchema s2 = ZooJdoHelper.schema(pm2);

		//data set 1
		ZooHandle[] t1 = new ZooHandle[5];
		long[] oids1 = new long[5];
		createData(pm1, t1, oids1);
		
		//concurrent modification
		TestSuper t21 = new TestSuper(21, 211, null);
		TestSuper t22 = new TestSuper(22, 222, null);
		TestSuper t23 = new TestSuper(23, 233, null);
		TestSuper t24 = new TestSuper(24, 244, null);
		TestSuper t25 = new TestSuper(25, 255, null);
		pm2.makePersistent(t21);
		pm2.makePersistent(t22);
		pm2.makePersistent(t23);
		pm2.makePersistent(t24);
		pm2.makePersistent(t25);
//		Object oid21 = pm2.getObjectId(t21);
//		Object oid25 = pm2.getObjectId(t25);
		
		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();

		pm2.currentTransaction().commit();
		pm2.currentTransaction().begin();

		try {
			//TODO remove this and implement test once we support setOid() 
			s2.getHandle(t21).setOid(1);
			fail();
		} catch (UnsupportedOperationException e) {
			
		}
		
//		//modified by both: t1, t3
//		//modified by 1: t4
//		//modified by 2: t5
//		s2.getHandle(t21).setOid((Long) oid1);
//		s2.getHandle(t23).setOid((long) oid3);
//		s2.getHandle(t23).remove();
//		s2.getHandle(t24).setOid((long) oid4);
////		t21.setId(21);
////		t23.setId(23);
////		t25.setId(25);
//		
//		s1.getHandle(t11).setOid((Long) oid21);
//		s1.getHandle(t14).setOid((long) oid3);
//		s1.getHandle(t14).remove();
//		s1.getHandle(t15).setOid((long) oid25);
////		t11.setId(11);
////		t13.setId(13);
////		t14.setId(14);
//		
//		pm1.currentTransaction().commit();
//		pm1.currentTransaction().begin();
//
//		DBStatistics stats = ZooJdoHelper.getStatistics(pm1);
//		System.out.println("s1_oids="+ stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));
//		System.out.println("s1_tx="+ stats.getStat(STATS.TX_MGR_BUFFERED_TX_CNT));
//		//5?
//		assertEquals(5, stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));
//		
//		try {
//			pm2.currentTransaction().commit();
//			fail();
//		} catch (JDOOptimisticVerificationException e) {
//			//good!
//			HashSet<Object> failedOids = new HashSet<>();
//			for (Throwable t: e.getNestedExceptions()) {
//				Object f = ((JDOOptimisticVerificationException)t).getFailedObject();
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
//		pm2.currentTransaction().begin();
//
//		//TODO revert pm1 and pm2... ?
//		t11.setId(t11.getId() + 1000);
//		t13.setId(t13.getId() + 1000);
//		t14.setId(14);
//		pm1.currentTransaction().commit();
//		pm1.currentTransaction().begin();
//
//		assertEquals(21+1000, t21.getId());
//		assertEquals(22, t22.getId());
//		assertEquals(23+1000, t23.getId());
//		assertEquals(14, t24.getId());
//		assertEquals(25, t25.getId());
//		
//
		pm1.currentTransaction().rollback();
		pm2.currentTransaction().rollback();
//		
//		//5?
//		assertEquals(5, stats.getStat(STATS.TX_MGR_BUFFERED_OID_CNT));

		pm1.close();
		pm2.close();
		pmf.close();
	}
	
	private void createData(PersistenceManager pm, ZooHandle[] data, long[] oids) {
		ZooSchema s = ZooJdoHelper.schema(pm);
		ZooClass c = s.getClass(TestSuper.class.getName());
		ZooHandle t10 = c.newInstance();
		t10.setValue("_time", 1L);
		t10.setValue("_id", 11L);
		ZooHandle t11 = c.newInstance();
		t11.setValue("_time", 2L);
		t11.setValue("_id", 22L);
		ZooHandle t12 = c.newInstance();
		t12.setValue("_time", 3L);
		t12.setValue("_id", 33L);
		ZooHandle t13 = c.newInstance();
		t13.setValue("_time", 4L);
		t13.setValue("_id", 44L);
		ZooHandle t14 = c.newInstance();
		t14.setValue("_time", 5L);
		t14.setValue("_id", 55L);
		
		data[0] = t10;
		data[1] = t11;
		data[2] = t12;
		data[3] = t13;
		data[4] = t14;
		
		oids[0] = t10.getOid();
		oids[1] = t11.getOid();
		oids[2] = t12.getOid();
		oids[2] = t13.getOid();
		oids[4] = t14.getOid();
	}
}

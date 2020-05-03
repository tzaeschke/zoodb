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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.jdo.Constants;
import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.ObjectState;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.listener.DetachLifecycleListener;
import javax.jdo.listener.InstanceLifecycleEvent;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.schema.ZooClass;
import org.zoodb.schema.ZooHandle;
import org.zoodb.schema.ZooSchema;
import org.zoodb.test.testutil.TestTools;

public class Test_130_DetachAllOnCommit {

	@BeforeClass
	public static void setUp() {
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
	}

	@Before
	public void before() {
		TestTools.dropInstances(TestClass.class);
	}

	@Test
	public void testPropertyPropagation() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf;
		PersistenceManager pm;

		//all false, vary pm
		assertEquals("false", props.getProperty(Constants.PROPERTY_DETACH_ALL_ON_COMMIT));
		pmf = JDOHelper.getPersistenceManagerFactory(props);
		assertFalse(pmf.getDetachAllOnCommit());
		pm = pmf.getPersistenceManager();
		assertFalse(pm.getDetachAllOnCommit());
		pm.setDetachAllOnCommit(true);
		assertTrue(pm.getDetachAllOnCommit());
		pm.setDetachAllOnCommit(false);
		assertFalse(pm.getDetachAllOnCommit());
		pm.close();
		
		//pmf true, vary pm
		pmf = JDOHelper.getPersistenceManagerFactory(props);
		pmf.setDetachAllOnCommit(true);
		assertTrue(pmf.getDetachAllOnCommit());
		pm = pmf.getPersistenceManager();
		assertTrue(pm.getDetachAllOnCommit());
		pm.setDetachAllOnCommit(false);
		assertFalse(pm.getDetachAllOnCommit());
		pm.setDetachAllOnCommit(true);
		assertTrue(pm.getDetachAllOnCommit());
		pm.close();
		
		//props true
		props.setDetachAllOnCommit(true);
		assertEquals("true", props.getProperty(Constants.PROPERTY_DETACH_ALL_ON_COMMIT));
		pmf = JDOHelper.getPersistenceManagerFactory(props);
		assertTrue(pmf.getDetachAllOnCommit());
	}


	@Test
	public void testDetachNewNoTx() {
		PersistenceManager pm = TestTools.openPM();
		pm.setDetachAllOnCommit(true);
		pm.currentTransaction().begin();
		
		TestClass tc1 = new TestClass();
		TestClass tc1b = new TestClass();
		pm.makePersistent(tc1);
		pm.makePersistent(tc1b);
		tc1.setInt(5);
		tc1.setRef2(tc1b);
		tc1b.setInt(6);

		//transitive pers.
		TestClass tc2 = new TestClass();
		TestClass tc2b = new TestClass();
		pm.makePersistent(tc2);
		tc2.setInt(7);
		tc2.setRef2(tc2b);
		tc2b.setInt(8);
		
		pm.currentTransaction().commit();

		//should work outside TX
		assertEquals(5, tc1.getInt());
		assertEquals(6, tc1b.getInt());
		assertEquals(7, tc2.getInt());
		assertEquals(8, tc2b.getInt());
		
		TestTools.closePM();
		
		//should work outside session
		assertEquals(5, tc1.getInt());
		assertEquals(6, tc1b.getInt());
		assertEquals(7, tc2.getInt());
		assertEquals(8, tc2b.getInt());
	}
	
	@Test
	public void testDetachNewNoSession() {
		PersistenceManager pm = TestTools.openPM();
		pm.setDetachAllOnCommit(true);
		pm.currentTransaction().begin();

		
		TestClass tc1 = new TestClass();
		TestClass tc1b = new TestClass();
		pm.makePersistent(tc1);
		pm.makePersistent(tc1b);
		tc1.setInt(5);
		tc1.setRef2(tc1b);
		tc1b.setInt(6);

		//transitive pers.
		TestClass tc2 = new TestClass();
		TestClass tc2b = new TestClass();
		pm.makePersistent(tc2);
		tc2.setInt(7);
		tc2.setRef2(tc2b);
		tc2b.setInt(8);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		//should work outside session
		assertEquals(5, tc1.getInt());
		assertEquals(6, tc1b.getInt());
		assertEquals(7, tc2.getInt());
		assertEquals(8, tc2b.getInt());
	}
	
	@Test
	public void testDetachNewNoTxRefs() {
		PersistenceManager pm = TestTools.openPM();
		pm.setDetachAllOnCommit(true);
		pm.currentTransaction().begin();

		
		TestClass tc1 = new TestClass();
		TestClass tc1b = new TestClass();
		pm.makePersistent(tc1);
		pm.makePersistent(tc1b);
		tc1.setInt(5);
		tc1.setRef2(tc1b);
		tc1b.setInt(6);

		//transitive pers.
		TestClass tc2 = new TestClass();
		TestClass tc2b = new TestClass();
		pm.makePersistent(tc2);
		tc2.setInt(7);
		tc2.setRef2(tc2b);
		tc2b.setInt(8);
		
		pm.currentTransaction().commit();

		//should work outside TX
		assertEquals(5, tc1.getInt());
		assertEquals(6, tc1.getRef2().getInt());
		assertEquals(7, tc2.getInt());
		assertEquals(8, tc2.getRef2().getInt());
		
		TestTools.closePM();
		
		//should work outside session
		assertEquals(5, tc1.getInt());
		assertEquals(6, tc1.getRef2().getInt());
		assertEquals(7, tc2.getInt());
		assertEquals(8, tc2.getRef2().getInt());
	}
	
	@Test
	public void testDetachNewNoSessionRefs() {
		PersistenceManager pm = TestTools.openPM();
		pm.setDetachAllOnCommit(true);
		pm.currentTransaction().begin();

		
		TestClass tc1 = new TestClass();
		TestClass tc1b = new TestClass();
		pm.makePersistent(tc1);
		pm.makePersistent(tc1b);
		tc1.setInt(5);
		tc1.setRef2(tc1b);
		tc1b.setInt(6);

		//transitive pers.
		TestClass tc2 = new TestClass();
		TestClass tc2b = new TestClass();
		pm.makePersistent(tc2);
		tc2.setInt(7);
		tc2.setRef2(tc2b);
		tc2b.setInt(8);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		//should work outside session
		assertEquals(5, tc1.getInt());
		assertEquals(6, tc1.getRef2().getInt());
		assertEquals(7, tc2.getInt());
		assertEquals(8, tc2.getRef2().getInt());
	}
	
	@Test
	public void testDetachPersNoTxRefs() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		
		TestClass tc1 = new TestClass();
		TestClass tc1b = new TestClass();
		pm.makePersistent(tc1);
		pm.makePersistent(tc1b);
		tc1.setInt(5);
		tc1.setRef2(tc1b);
		tc1b.setInt(6);

		TestClass tc2 = new TestClass();
		TestClass tc2b = new TestClass();
		pm.makePersistent(tc2);
		tc2.setInt(7);

		pm.currentTransaction().commit();
		pm.setDetachAllOnCommit(true);
		pm.currentTransaction().begin();

		//dirty some of them
		tc2.setRef2(tc2b);
		tc2b.setInt(8);
		
		
		pm.currentTransaction().commit();

		//should work outside TX
		assertEquals(5, tc1.getInt());
		assertEquals(6, tc1.getRef2().getInt());
		assertEquals(7, tc2.getInt());
		assertEquals(8, tc2.getRef2().getInt());
		
		TestTools.closePM();
		
		//should work outside session
		assertEquals(5, tc1.getInt());
		assertEquals(6, tc1.getRef2().getInt());
		assertEquals(7, tc2.getInt());
		assertEquals(8, tc2.getRef2().getInt());
	}
	
	@Test
	public void testDetachPersNoSessionRefs() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		
		TestClass tc1 = new TestClass();
		TestClass tc1b = new TestClass();
		pm.makePersistent(tc1);
		pm.makePersistent(tc1b);
		tc1.setInt(5);
		tc1.setRef2(tc1b);
		tc1b.setInt(6);

		//transitive pers.
		TestClass tc2 = new TestClass();
		TestClass tc2b = new TestClass();
		pm.makePersistent(tc2);
		tc2.setInt(7);

		pm.currentTransaction().commit();
		pm.setDetachAllOnCommit(true);
		pm.currentTransaction().begin();

		tc2.setRef2(tc2b);
		tc2b.setInt(8);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		//should work outside session
		assertEquals(5, tc1.getInt());
		assertEquals(6, tc1.getRef2().getInt());
		assertEquals(7, tc2.getInt());
		assertEquals(8, tc2.getRef2().getInt());
	}
	
	@Test
	public void testMakePersistent() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClass tc1 = new TestClass();
		TestClass tc1b = new TestClass();
		pm.makePersistent(tc1);
		pm.makePersistent(tc1b);
		tc1.setInt(5);
		tc1.setRef2(tc1b);
		tc1b.setInt(6);

		//transitive pers.
		TestClass tc2 = new TestClass();
		TestClass tc2b = new TestClass();
		pm.makePersistent(tc2);
		tc2.setInt(7);

		pm.currentTransaction().commit();
		pm.setDetachAllOnCommit(true);
		pm.currentTransaction().begin();

		tc2.setRef2(tc2b);
		tc2b.setInt(8);
		
		pm.currentTransaction().commit();

		
		tc1.setInt(50);
		tc1b.setInt(60);
		tc2.setInt(70);
		tc2b.setInt(80);

		pm.setDetachAllOnCommit(false);
		pm.currentTransaction().begin();

		pm.makePersistent(tc1);
		pm.makePersistent(tc2);
		Object o1 = JDOHelper.getObjectId(tc1);
		Object o2 = JDOHelper.getObjectId(tc2);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		pm = TestTools.openPM();
		pm.currentTransaction().begin();

		TestClass tc11 = (TestClass) pm.getObjectById(o1);
		TestClass tc21 = (TestClass) pm.getObjectById(o2);
		
		//should work outside session
		assertEquals(50, tc11.getInt());
		assertEquals(60, tc11.getRef2().getInt());
		assertEquals(70, tc21.getInt());
		assertEquals(80, tc21.getRef2().getInt());
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}
	
	/**
	 * Test that makePersistent works transitively.
	 */
	@Test
	public void testMakePersistentTransitive() {
		PersistenceManager pm = TestTools.openPM();
		pm.setDetachAllOnCommit(true);
		pm.currentTransaction().begin();
		
		TestClass tc1 = new TestClass();
		TestClass tc1b = new TestClass();
		pm.makePersistent(tc1);
		pm.makePersistent(tc1b);
		tc1.setInt(5);
		tc1.setRef2(tc1b);
		tc1b.setInt(6);

		//transitive pers.
		TestClass tc2 = new TestClass();
		TestClass tc2b = new TestClass();
		pm.makePersistent(tc2);
		tc2.setInt(7);

		//detach all
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//Now modify and check that everything gets stored
		tc1b.setInt(18);
		
		tc2.setRef2(tc2b);
		tc2b.setInt(28);

		//add new instances
		TestClass tc1c = new TestClass();
		tc1c.setInt(188);
		tc1b.setRef2(tc1c);
		TestClass tc2c = new TestClass();
		tc2c.setInt(288);
		tc2b.setRef2(tc2c);

		//reattach
		pm.makePersistent(tc1);
		pm.makePersistent(tc2);
		Object o1 = JDOHelper.getObjectId(tc1);
		Object o2 = JDOHelper.getObjectId(tc2);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		pm = TestTools.openPM();
		pm.currentTransaction().begin();

		TestClass tc11 = (TestClass) pm.getObjectById(o1);
		TestClass tc21 = (TestClass) pm.getObjectById(o2);
		
		assertEquals(5, tc11.getInt());
		assertEquals(18, tc11.getRef2().getInt());
		assertEquals(188, tc11.getRef2().getRef2().getInt());
		assertEquals(7, tc21.getInt());
		assertEquals(28, tc21.getRef2().getInt());
		assertEquals(288, tc21.getRef2().getRef2().getInt());
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}
	
	/**
	 * Test that makePersistent does duplicate objects.
	 * Duplication is intended (well, tolerated) behaviour, see issue 75
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testDuplication_Issue_075() {
		PersistenceManager pm = TestTools.openPM();
		pm.setDetachAllOnCommit(true);
		pm.currentTransaction().begin();
		
		TestClass tc1 = new TestClass();
		TestClass tc1b = new TestClass();
		pm.makePersistent(tc1);
		pm.makePersistent(tc1b);
		tc1.setInt(5);
		tc1.setRef2(tc1b);
		tc1b.setInt(6);

		//detach all
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		Query q = pm.newQuery(TestClass.class);
		q.setUnique(true);
		q.setFilter("_int == 5");
		q.execute();
		q.setFilter("_int == 6");
		TestClass x2 = (TestClass) q.execute();

		TestClass x3 = new TestClass();
		pm.makePersistent(x3);
		x3.setInt(56);
		//make Persistent via reachability
		x3.setRef2(x2);
		
		//No modify and check that everything gets stored
		tc1b.setInt(18);

		//The following behavior changed!
		//Originally (See Issue #75) this was implemented such that a new OID
		//is assigned. This has been changed (See Issue #112) such that
		//Reattach is not possible if another object with the same OID is already in the cache. 
		
		//reattach
		try {
			pm.makePersistent(tc1);
		} catch (JDOUserException e) {
			
		}
		try {
			pm.makePersistent(tc1b);
		} catch (JDOUserException e) {
			
		}
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q2 = pm.newQuery(TestClass.class);
		Collection<TestClass> c = (Collection<TestClass>) q2.execute();
		
		//OLD detach causes duplication, apparently this is intended, see issue 75
		//assertEquals(5, c.size());
		//NEW detach causes duplication, apparently this is intended, see issue 112
		assertEquals(3, c.size());
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}
	
	@Test
	public void testCacheIsEmpty() {
		PersistenceManager pm = TestTools.openPM();
		pm.setDetachAllOnCommit(true);
		pm.currentTransaction().begin();
		
		TestClass tc1 = new TestClass();
		TestClass tc1b = new TestClass();
		pm.makePersistent(tc1);
		pm.makePersistent(tc1b);
		tc1.setInt(5);
		tc1.setRef2(tc1b);
		tc1b.setInt(6);

		Set<?> cache = pm.getManagedObjects();
		assertTrue(cache.contains(tc1));
		assertTrue(cache.contains(tc1b));

		//detach
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		cache = pm.getManagedObjects();
		assertFalse(cache.contains(tc1));
		assertFalse(cache.contains(tc1b));
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
	@Test
	public void testNoDetachSchema() {
		PersistenceManager pm = TestTools.openPM();
		pm.setDetachAllOnCommit(true);
		pm.currentTransaction().begin();
		
		ZooSchema s = ZooJdoHelper.schema(pm);
		s.addClass(TestClassTiny.class);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		assertNotNull(s.getClass(TestClassTiny.class));
		try {
			s.addClass(TestClassTiny.class);
			fail();
		} catch (JDOUserException e) {
			//good, is already defined
		}
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
	@Test
	public void testNoDetachGenericObjects() {
		PersistenceManager pm = TestTools.openPM();
		pm.setDetachAllOnCommit(true);
		pm.currentTransaction().begin();
		
		ZooSchema s = ZooJdoHelper.schema(pm);
		ZooClass c = s.getClass(TestClass.class);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		ZooHandle h = c.newInstance();
		Object oid = h.getOid();
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		assertNotNull(pm.getObjectById(oid));
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
	private static class ListenerDetach implements DetachLifecycleListener {
		final Set<TestClass> instances = new HashSet<>();
		int preDetach = 0;
		int postDetach = 0;
		@Override
		public void preDetach(InstanceLifecycleEvent arg0) {
			assertEquals(InstanceLifecycleEvent.DETACH, arg0.getEventType());
			instances.add((TestClass) arg0.getDetachedInstance());
			preDetach++;
		}
		@Override
		public void postDetach(InstanceLifecycleEvent arg0) {
			assertEquals(InstanceLifecycleEvent.DETACH, arg0.getEventType());
			instances.add((TestClass) arg0.getDetachedInstance());
			postDetach++;
		}
	}
	
	@Test
	public void testCallbacks() {
		PersistenceManager pm = TestTools.openPM();
		pm.setDetachAllOnCommit(true);
		
		ListenerDetach l = new ListenerDetach();
		pm.addInstanceLifecycleListener(l, TestClass.class);
		
		pm.currentTransaction().begin();

		TestClass tc1 = new TestClass();
		TestClass tc1b = new TestClass();
		pm.makePersistent(tc1);
		tc1.setInt(5);
		tc1.setRef2(tc1b);
		tc1b.setInt(6);

		//detach
		pm.currentTransaction().commit();
		
		assertEquals(2, l.preDetach);
		assertEquals(2, l.postDetach);
		assertTrue(l.instances.contains(tc1));
		assertTrue(l.instances.contains(tc1b));
		
		TestTools.closePM();
	}
	
	@Test
	public void testTransitionToDetachedDirty() {
		PersistenceManager pm = TestTools.openPM();
		pm.setDetachAllOnCommit(true);
		pm.currentTransaction().begin();
		
		TestClass tc1 = new TestClass();
		TestClass tc1b = new TestClass();
		pm.makePersistent(tc1);
		pm.makePersistent(tc1b);
		tc1.setInt(5);
		tc1.setRef2(tc1b);
		tc1b.setInt(6);

		//detach
		pm.currentTransaction().commit();
		TestTools.closePM();

		assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(tc1));
		assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(tc1b));
		
		tc1.setRef2(null);
		tc1b.setInt(60);

		assertEquals(ObjectState.DETACHED_DIRTY, JDOHelper.getObjectState(tc1));
		assertEquals(ObjectState.DETACHED_DIRTY, JDOHelper.getObjectState(tc1b));
	}
	
	@Test
	public void testDetachedOID() {
		PersistenceManager pm = TestTools.openPM();
		pm.setDetachAllOnCommit(true);
		pm.currentTransaction().begin();
		
		TestClass tc1 = new TestClass();
		TestClass tc1b = new TestClass();
		pm.makePersistent(tc1);
		pm.makePersistent(tc1b);
		tc1.setInt(5);
		tc1.setRef2(tc1b);
		tc1b.setInt(6);

		Object o1 = JDOHelper.getObjectId(tc1);
		Object o1b = JDOHelper.getObjectId(tc1b);

		//detach
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//reattach
		pm.makePersistent(tc1);
		pm.makePersistent(tc1b);

		//Check that oids are still good
		Object o21 = JDOHelper.getObjectId(tc1);
		Object o21b = JDOHelper.getObjectId(tc1b);

		assertEquals(o1, o21);
		assertEquals(o1b, o21b);
		assertTrue(((Long)o21) > 0);
		assertTrue(((Long)o21b) > 0);

		//See spec Figure 14.0
		tc1b.setInt(1222);
		assertEquals(ObjectState.PERSISTENT_CLEAN, JDOHelper.getObjectState(tc1));
		assertEquals(ObjectState.PERSISTENT_DIRTY, JDOHelper.getObjectState(tc1b));
		
		pm.currentTransaction().commit();

		TestTools.closePM();
	}
	
	@Test
	public void testReattachCollision() {
		//Test that collision during reattach are handled properly
		PersistenceManager pm = TestTools.openPM();
		pm.setDetachAllOnCommit(true);
		pm.currentTransaction().begin();
		
		TestClass tc1 = new TestClass();
		TestClass tc1b = new TestClass();
		pm.makePersistent(tc1);
		pm.makePersistent(tc1b);
		tc1.setInt(5);
		tc1.setRef2(tc1b);
		tc1b.setInt(6);

		Object o1 = JDOHelper.getObjectId(tc1);
		Object o1b = JDOHelper.getObjectId(tc1b);

		//detach
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//load object from DB
		TestClass tc2 = (TestClass) pm.getObjectById(o1);
		TestClass tc2b = tc2.getRef2();
		
		//Check that oids are still good
		Object o2 = JDOHelper.getObjectId(tc1);
		Object o2b = JDOHelper.getObjectId(tc1b);
		assertEquals(o1, o2);
		assertEquals(o1b, o2b);
		assertTrue(((Long)o2) > 0);
		assertTrue(((Long)o2b) > 0);

		//Test that objects are distinct
		assertTrue(tc1 != tc2);
		assertTrue(tc1b != tc2b);
		
		//test reattach
		try {
			//reattach
			pm.makePersistent(tc1);
			pm.makePersistent(tc1b);
			fail();
		} catch (JDOUserException e) {
			assertTrue(e.getMessage().contains("OID conflict"));
		}
		
		assertTrue(JDOHelper.isDetached(tc1));
		assertTrue(JDOHelper.isDetached(tc1b));
		assertFalse(JDOHelper.isDetached(tc2));
		assertFalse(JDOHelper.isDetached(tc2b));
		
		pm.currentTransaction().commit();

		TestTools.closePM();
	}
	
	@Test
	public void testReattachTransitiveCollision() {
		//Test that collision during transitive reattach are handled properly
		//Transitive: not explicitly via makePersistent() but transitively.
		PersistenceManager pm = TestTools.openPM();
		pm.setDetachAllOnCommit(true);
		pm.currentTransaction().begin();
		
		TestClass tc1 = new TestClass();
		TestClass tc1b = new TestClass();
		pm.makePersistent(tc1);
		pm.makePersistent(tc1b);
		tc1.setInt(5);
		tc1.setRef2(tc1b);
		tc1b.setInt(6);

		Object o1 = JDOHelper.getObjectId(tc1);
		Object o1b = JDOHelper.getObjectId(tc1b);

		//detach
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//load object from DB
		TestClass tc2 = (TestClass) pm.getObjectById(o1);
		TestClass tc2b = tc2.getRef2();
		
		//Check that oids are still good
		Object o21 = JDOHelper.getObjectId(tc1);
		Object o21b = JDOHelper.getObjectId(tc1b);
		assertEquals(o1, o21);
		assertEquals(o1b, o21b);
		assertTrue(((Long)o21) > 0);
		assertTrue(((Long)o21b) > 0);

		//Test that objects are distinct
		assertTrue(tc1 != tc2);
		assertTrue(tc1b != tc2b);
		
		//test transitive reattach
		tc2.setRef2(tc1b);
	
		try {
			pm.currentTransaction().commit();
			fail();
		} catch (JDOUserException e) {
			assertTrue(e.getMessage().contains("OID conflict"));
		}

		//back replace with original 
		tc2.setRef2(tc2b);

		assertTrue(JDOHelper.isDetached(tc1));
		assertTrue(JDOHelper.isDetached(tc1b));
		assertFalse(JDOHelper.isDetached(tc2));
		assertFalse(JDOHelper.isDetached(tc2b));
		
		pm.currentTransaction().commit();

		TestTools.closePM();
	}
	
	
	@Test
	public void testReattachDirty() {
		//Test that the dirty flag is preserved during reattach.
		PersistenceManager pm = TestTools.openPM();
		pm.setDetachAllOnCommit(true);
		pm.currentTransaction().begin();
		
		TestClass tc1 = new TestClass();
		TestClass tc2 = new TestClass();
		pm.makePersistent(tc1);
		pm.makePersistent(tc2);
		tc1.setInt(5);
		tc2.setInt(6);

		//detach
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//make dirty
		tc2.setInt(11);
		
		//reattach
		pm.makePersistent(tc1);
		pm.makePersistent(tc2);

		assertEquals(ObjectState.PERSISTENT_CLEAN, JDOHelper.getObjectState(tc1));
		assertEquals(ObjectState.PERSISTENT_DIRTY, JDOHelper.getObjectState(tc2));
		
		pm.currentTransaction().commit();
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

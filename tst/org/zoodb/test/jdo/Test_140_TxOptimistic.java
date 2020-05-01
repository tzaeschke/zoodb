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
import static org.junit.Assert.assertTrue;

import javax.jdo.Constants;
import javax.jdo.JDOHelper;
import javax.jdo.ObjectState;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.test.testutil.TestTools;

public class Test_140_TxOptimistic {

	private ZooJdoProperties props;
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
	}

	@Before
	public void before() {
		TestTools.dropInstances(TestClass.class);
		props = TestTools.getProps();
		//TODO
		//props.setOptimistic(true);
	}
	
	@After
	public void afterTest() {
		TestTools.closePM();
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testPropertyPropagation() {
		//TODO do in begin() only
		props.setOptimistic(true);
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf;

		//all false, vary pm
		assertEquals("false", props.getProperty(Constants.PROPERTY_OPTIMISTIC));
		pmf = JDOHelper.getPersistenceManagerFactory(props);
		assertFalse(pmf.getOptimistic());
		
		//pmf true, vary pm
		pmf = JDOHelper.getPersistenceManagerFactory(props);
		pmf.setOptimistic(true);
		assertTrue(pmf.getOptimistic());
		
		//props true
		props.setOptimistic(true);
		assertEquals("true", props.getProperty(Constants.PROPERTY_OPTIMISTIC));
		pmf = JDOHelper.getPersistenceManagerFactory(props);
		assertTrue(pmf.getOptimistic());
	}


	@Test(expected=UnsupportedOperationException.class)
	public void testStateTransitions() {
		//TODO do in begin() only
		props.setOptimistic(true);
		PersistenceManager pm = TestTools.openPM(props);
		pm.currentTransaction().begin();
		
		assertStateTransition(pm, ObjectState.PERSISTENT_NEW, 
				ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL);
		assertStateTransition(pm, ObjectState.PERSISTENT_CLEAN, 
				ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL);
		assertStateTransition(pm, ObjectState.PERSISTENT_DELETED, 
				ObjectState.TRANSIENT);
		assertStateTransition(pm, ObjectState.PERSISTENT_DIRTY, 
				ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL);
		assertStateTransition(pm, ObjectState.PERSISTENT_NEW_DELETED, 
				ObjectState.TRANSIENT);
		assertStateTransition(pm, ObjectState.PERSISTENT_NONTRANSACTIONAL_DIRTY, 
				ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL);
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}
	
	@Test(expected=UnsupportedOperationException.class)
	public void testNoDetachSchema() {
		//TODO do in begin() only
		props.setOptimistic(true);
		//TODO implement
//		PersistenceManager pm = TestTools.openPM(props);
//		pm.setDetachAllOnCommit(true);
//		pm.currentTransaction().begin();
//		
//		ZooSchema s = ZooJdoHelper.schema(pm);
//		s.addClass(TestClassTiny.class);
//		
//		pm.currentTransaction().commit();
//		pm.currentTransaction().begin();
//		
//		assertNotNull(s.getClass(TestClassTiny.class));
//		try {
//			s.addClass(TestClassTiny.class);
//			fail();		
//		} catch (JDOUserException e) {
//			//good, is already defined
//		}
//		
//		pm.currentTransaction().commit();
//		TestTools.closePM();
	}
	
	@Test(expected=UnsupportedOperationException.class)
	public void testNoDetachGenericObjects() {
		//TODO do in begin() only
		props.setOptimistic(true);
		//TODO implement
//		PersistenceManager pm = TestTools.openPM(props);
//		pm.setDetachAllOnCommit(true);
//		pm.currentTransaction().begin();
//		
//		ZooSchema s = ZooJdoHelper.schema(pm);
//		ZooClass c = s.getClass(TestClass.class);
//		
//		pm.currentTransaction().commit();
//		pm.currentTransaction().begin();
//
//		ZooHandle h = c.newInstance();
//		Object oid = h.getOid();
//		
//		pm.currentTransaction().commit();
//		pm.currentTransaction().begin();
//		
//		assertNotNull(pm.getObjectById(oid));
//		
//		pm.currentTransaction().commit();
//		TestTools.closePM();
	}
	
	@Test(expected=UnsupportedOperationException.class)
	public void testTransitionToDetachedDirty() {
		//TODO do in begin() only
		props.setOptimistic(true);
		//TODO implement
//		PersistenceManager pm = TestTools.openPM(props);
//		pm.setDetachAllOnCommit(true);
//		pm.currentTransaction().begin();
//		
//		TestClass tc1 = new TestClass();
//		TestClass tc1b = new TestClass();
//		pm.makePersistent(tc1);
//		pm.makePersistent(tc1b);
//		tc1.setInt(5);
//		tc1.setRef2(tc1b);
//		tc1b.setInt(6);
//
//		//detach
//		pm.currentTransaction().commit();
//		TestTools.closePM();
//
//		assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(tc1));
//		assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(tc1b));
//		
//		tc1.setRef2(null);
//		tc1b.setInt(60);
//
//		assertEquals(ObjectState.DETACHED_DIRTY, JDOHelper.getObjectState(tc1));
//		assertEquals(ObjectState.DETACHED_DIRTY, JDOHelper.getObjectState(tc1b));
	}
	
	
	private TestClass createObj(PersistenceManager pm, ObjectState state) {
		TestClass t = new TestClass();
		if (state == ObjectState.TRANSIENT) {
			return t;
		}
		
		pm.makePersistent(t);
		
		switch(state) {
		case PERSISTENT_NEW: return t;
		case PERSISTENT_NEW_DELETED: pm.deletePersistent(t); return t;
		default:
		}
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		switch(state) {
		case DETACHED_CLEAN: t = pm.detachCopy(t); break;
		case DETACHED_DIRTY: t = pm.detachCopy(t); t.setInt(t.getInt()+1); break;
		case HOLLOW_PERSISTENT_NONTRANSACTIONAL: break;
		case PERSISTENT_CLEAN: break;
		case PERSISTENT_DELETED: pm.deletePersistent(t); break;
		case PERSISTENT_DIRTY: t.setInt(t.getInt()+1); break;
		case PERSISTENT_NONTRANSACTIONAL_DIRTY: pm.makeNontransactional(t);
		default: throw new UnsupportedOperationException(state.name());
		}
		return t;
	}
	
	private void assertState(TestClass pc, ObjectState state) {
		assertEquals(state, JDOHelper.getObjectState(pc));
	}

	private void assertStateTransition(
			PersistenceManager pm, ObjectState before, ObjectState after) {
		TestClass pc = createObj(pm, before);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		assertState(pc, after);
	}


}

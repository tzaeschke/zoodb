/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
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

public class Test_150_TxNonTxRead {

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
		props.setNontransactionalRead(true);
	}
	
	@After
	public void afterTest() {
		TestTools.closePM();
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
	}

	@Test
	public void testPropertyPropagation() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf;

		//all false, vary pm
		assertEquals("false", props.getProperty(Constants.PROPERTY_NONTRANSACTIONAL_READ));
		pmf = JDOHelper.getPersistenceManagerFactory(props);
		assertFalse(pmf.getNontransactionalRead());
		
		//pmf true, vary pm
		pmf = JDOHelper.getPersistenceManagerFactory(props);
		pmf.setNontransactionalRead(true);;
		assertTrue(pmf.getNontransactionalRead());
		
		//props true
		props.setNontransactionalRead(true);
		assertEquals("true", props.getProperty(Constants.PROPERTY_NONTRANSACTIONAL_READ));
		pmf = JDOHelper.getPersistenceManagerFactory(props);
		assertTrue(pmf.getNontransactionalRead());
	}


	@Test
	public void testNoDetachSchema() {
		fail(); //TODO implement
//		PersistenceManager pm = TestTools.openPM();
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
//		} catch (JDOUserException e) {
//			//good, is already defined
//		}
//		
//		pm.currentTransaction().commit();
//		TestTools.closePM();
	}
	
	@Test
	public void testNoDetachGenericObjects() {
		fail(); //TODO implement
//		PersistenceManager pm = TestTools.openPM();
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
	
	@Test
	public void testTransitionToDetachedDirty() {
		fail(); //TODO implement
//		PersistenceManager pm = TestTools.openPM();
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

	@Test
	public void testRead() {
		PersistenceManager pm = TestTools.openPM(props);
		pm.currentTransaction().begin();
		
		TestClass tc1 = new TestClass();
		TestClass tc1b = new TestClass();
		pm.makePersistent(tc1);
		pm.makePersistent(tc1b);
		tc1.setInt(5);
		tc1.setRef2(tc1b);
		tc1b.setInt(6);
		
		pm.currentTransaction().commit();
		
		
		assertEquals(5, tc1.getInt());
		assertEquals(6, tc1.getRef2().getInt());
		
		
		
		pm.currentTransaction().begin();
		pm.currentTransaction().commit();
		
		TestTools.closePM();
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

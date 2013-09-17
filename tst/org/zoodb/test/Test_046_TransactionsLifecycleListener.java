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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.listener.ClearLifecycleListener;
import javax.jdo.listener.CreateLifecycleListener;
import javax.jdo.listener.DeleteLifecycleListener;
import javax.jdo.listener.DirtyLifecycleListener;
import javax.jdo.listener.InstanceLifecycleEvent;
import javax.jdo.listener.LoadLifecycleListener;
import javax.jdo.listener.StoreLifecycleListener;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.api.ZooInstanceEvent;
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.test.testutil.TestTools;

public class Test_046_TransactionsLifecycleListener {

	private static final ArrayList<Pair> calls = new ArrayList<Pair>();
	
	private static final class Pair {
		final ZooInstanceEvent type;
		final InstanceLifecycleEvent e;
		public Pair(InstanceLifecycleEvent e, ZooInstanceEvent type) {
			this.e = e;
			this.type = type;
		}
	}
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
	}

	@Before
	public void before() {
		TestTools.dropInstances(TestClass.class);
		calls.clear();
	}

	private static class ListenerLoad implements LoadLifecycleListener {
		@Override
		public void postLoad(InstanceLifecycleEvent arg0) {
			registerCall(arg0, ZooInstanceEvent.LOAD);			
		}
	}
	
	private static class ListenerStore implements StoreLifecycleListener {
		@Override
		public void postStore(InstanceLifecycleEvent arg0) {
			registerCall(arg0, ZooInstanceEvent.POST_STORE);			
		}
		@Override
		public void preStore(InstanceLifecycleEvent arg0) {
			registerCall(arg0, ZooInstanceEvent.PRE_STORE);			
		}
	}
	
	private static class ListenerClear implements ClearLifecycleListener {
		@Override
		public void postClear(InstanceLifecycleEvent arg0) {
			registerCall(arg0, ZooInstanceEvent.POST_CLEAR);			
		}
		@Override
		public void preClear(InstanceLifecycleEvent arg0) {
			registerCall(arg0, ZooInstanceEvent.PRE_CLEAR);			
		}
	}
	
	private static class ListenerDelete implements DeleteLifecycleListener {
		@Override
		public void postDelete(InstanceLifecycleEvent arg0) {
			registerCall(arg0, ZooInstanceEvent.POST_DELETE);			
		}
		@Override
		public void preDelete(InstanceLifecycleEvent arg0) {
			registerCall(arg0, ZooInstanceEvent.PRE_DELETE);			
		}
	}
	
	private static class ListenerCreate implements CreateLifecycleListener {
		@Override
		public void postCreate(InstanceLifecycleEvent arg0) {
			registerCall(arg0, ZooInstanceEvent.CREATE);			
		}
	}
	
	private static class ListenerDirty implements DirtyLifecycleListener {
		@Override
		public void postDirty(InstanceLifecycleEvent arg0) {
			registerCall(arg0, ZooInstanceEvent.POST_DIRTY);			
		}
		@Override
		public void preDirty(InstanceLifecycleEvent arg0) {
			registerCall(arg0, ZooInstanceEvent.PRE_DIRTY);			
		}
	}
	
	@Test
	public void testLifecycleListenerFail() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		try {
			pm.addInstanceLifecycleListener(new ListenerClear(), TestClassTinyClone.class);
		} catch (JDOUserException e) {
			//good, this should NOT throw a NPE.
		}
		try {
			pm.addInstanceLifecycleListener(new ListenerClear(), (Class<?>)null);
		} catch (JDOUserException e) {
			//good, this should NOT throw a NPE.
		}
		try {
			pm.addInstanceLifecycleListener(new ListenerClear(), (Class<?>[])null);
		} catch (JDOUserException e) {
			//good, this should NOT throw a NPE.
		}
		
		TestTools.closePM();
	} 
	
	@Test
	public void testLifecycleListener() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		pm.addInstanceLifecycleListener(new ListenerClear(), TestClass.class);
		pm.addInstanceLifecycleListener(new ListenerCreate(), TestClass.class);
		pm.addInstanceLifecycleListener(new ListenerDelete(), TestClass.class);
		pm.addInstanceLifecycleListener(new ListenerDirty(), TestClass.class);
		pm.addInstanceLifecycleListener(new ListenerLoad(), TestClass.class);
		pm.addInstanceLifecycleListener(new ListenerStore(), TestClass.class);
		
		internalTest(pm);
		
		TestTools.closePM();
	} 
	
	@Test
	public void testLifecycleListenerNull() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		pm.addInstanceLifecycleListener(new ListenerClear(), (Class<?>)null);
		pm.addInstanceLifecycleListener(new ListenerCreate(), (Class<?>)null);
		pm.addInstanceLifecycleListener(new ListenerDelete(), (Class<?>)null);
		pm.addInstanceLifecycleListener(new ListenerDirty(), (Class<?>)null);
		pm.addInstanceLifecycleListener(new ListenerLoad(), (Class<?>)null);
		pm.addInstanceLifecycleListener(new ListenerStore(), (Class<?>)null);
		
		internalTest(pm);
		
		TestTools.closePM();
	} 
	
	@Test
	public void testLifecycleListenerPMF() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
		
		Class<?>[] clsA = new Class[]{TestClass.class}; 
		pmf.addInstanceLifecycleListener(new ListenerClear(), clsA);
		pmf.addInstanceLifecycleListener(new ListenerCreate(), clsA);
		pmf.addInstanceLifecycleListener(new ListenerDelete(), clsA);
		pmf.addInstanceLifecycleListener(new ListenerDirty(), clsA);
		pmf.addInstanceLifecycleListener(new ListenerLoad(), clsA);
		pmf.addInstanceLifecycleListener(new ListenerStore(), clsA);
		
		PersistenceManager pm = pmf.getPersistenceManager();
		pm.currentTransaction().begin();
		internalTest(pm);
		TestTools.closePM(pm);
	} 
	
	@Test
	public void testLifecycleListenerPmfNull() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
		
		pmf.addInstanceLifecycleListener(new ListenerClear(), null);
		pmf.addInstanceLifecycleListener(new ListenerCreate(), null);
		pmf.addInstanceLifecycleListener(new ListenerDelete(), null);
		pmf.addInstanceLifecycleListener(new ListenerDirty(), null);
		pmf.addInstanceLifecycleListener(new ListenerLoad(), null);
		pmf.addInstanceLifecycleListener(new ListenerStore(), null);
		
		PersistenceManager pm = pmf.getPersistenceManager();
		pm.currentTransaction().begin();
		internalTest(pm);
		TestTools.closePM(pm);
	} 
	
	private void internalTest(PersistenceManager pm) { 
		
		TestClass t1 = new TestClass();
		//to check clear()
		t1.setRef2(t1);

		assertTrue(calls.isEmpty());

		//check CREATE
		pm.makePersistent(t1);
		checkCall(ZooInstanceEvent.CREATE, t1);
		assertTrue(calls.isEmpty());
		
		//check STORE
		pm.currentTransaction().commit();
		checkCall(ZooInstanceEvent.PRE_STORE, t1);  //preStore
		checkCall(ZooInstanceEvent.PRE_CLEAR, t1);  
		checkCall(ZooInstanceEvent.POST_CLEAR, t1);  
		checkCall(ZooInstanceEvent.POST_STORE, t1);  //postStore
		assertTrue(calls.isEmpty());
		
		pm.currentTransaction().begin();

		// check LOAD
		assertNotNull(t1.getRef2());
		checkCall(ZooInstanceEvent.LOAD, t1);  
		assertTrue(calls.isEmpty());
		
		// check modify
		t1.setRef2(null);
		checkCall(ZooInstanceEvent.PRE_DIRTY, t1);
		checkCall(ZooInstanceEvent.POST_DIRTY, t1);
		System.err.println("WARNING InstanceLifecycles are currently not correct. " +
				"PRE_DIRTY and POST_DIRTY should be triggered BEFORE/AFTER a field is modified.");
		assertTrue(calls.isEmpty());

		//check refresh-LOAD
		pm.refresh(t1);
		checkCall(ZooInstanceEvent.LOAD, t1);  
		assertTrue(calls.isEmpty());
		
		//check DELETE
		pm.deletePersistent(t1);
		assertTrue(calls.isEmpty());
		pm.currentTransaction().commit();
		checkCall(ZooInstanceEvent.PRE_DELETE, t1);
		checkCall(ZooInstanceEvent.POST_DELETE, t1);
		assertTrue(calls.isEmpty());
	}

	@Test
	public void testLifecycleListenerRemoval() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		ListenerCreate lc = new ListenerCreate();
		pm.addInstanceLifecycleListener(lc, TestClass.class);
		
		TestClass t1 = new TestClass();
		assertTrue(calls.isEmpty());

		//check CREATE
		pm.makePersistent(t1);
		checkCall(ZooInstanceEvent.CREATE, t1);
		assertTrue(calls.isEmpty());
		
		//remove listener
		pm.removeInstanceLifecycleListener(lc);
		
		TestClass t2 = new TestClass();
		pm.makePersistent(t2);
		assertTrue(calls.isEmpty());
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}
	
	@Test
	public void testLifecycleListenerRemovalPmf() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);

		ListenerCreate lc = new ListenerCreate();
		pmf.addInstanceLifecycleListener(lc, new Class[]{TestClass.class});
		pmf.removeInstanceLifecycleListener(lc);

		PersistenceManager pm = pmf.getPersistenceManager();
		pm.currentTransaction().begin();
		
		TestClass t1 = new TestClass();
		assertTrue(calls.isEmpty());

		//check CREATE
		pm.makePersistent(t1);
		assertTrue(calls.isEmpty());
		
		//remove listener
		TestClass t2 = new TestClass();
		pm.makePersistent(t2);
		assertTrue(calls.isEmpty());
		
		pm.currentTransaction().rollback();
		TestTools.closePM(pm);
	}
	
	private void checkCall(ZooInstanceEvent expected, Object pc) {
		assertTrue(calls.size() > 0);
		Pair p = calls.get(0); 
		System.out.println("calls:" + calls.size() + "  " + p.e.getPersistentInstance());
		System.out.println("calls:" + p.type + "  " + p.e.getPersistentInstance().getClass());
		assertEquals(expected, p.type);
		assertEquals(TestClass.class, p.e.getSource().getClass());
		assertTrue(pc == p.e.getPersistentInstance());
		calls.remove(0);
	}
	
	private static void registerCall(InstanceLifecycleEvent e, ZooInstanceEvent type) {
		calls.add(new Pair(e, type));
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

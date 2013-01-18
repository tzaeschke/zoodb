/*
 * Copyright 2009-2012 Tilmann Zäschke. All rights reserved.
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

import javax.jdo.PersistenceManager;
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
import org.zoodb.test.util.TestTools;

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
	public void testLifecycleListener() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		pm.addInstanceLifecycleListener(new ListenerClear(), TestClass.class);
		pm.addInstanceLifecycleListener(new ListenerCreate(), TestClass.class);
		pm.addInstanceLifecycleListener(new ListenerDelete(), TestClass.class);
		pm.addInstanceLifecycleListener(new ListenerDirty(), TestClass.class);
		pm.addInstanceLifecycleListener(new ListenerLoad(), TestClass.class);
		pm.addInstanceLifecycleListener(new ListenerStore(), TestClass.class);
		
		TestClass t1 = new TestClass();
		//to check clear()
		t1.setRef2(t1);

		assertTrue(calls.isEmpty());

		//check CREATE
		pm.makePersistent(t1);
		checkCall(ZooInstanceEvent.CREATE);
		assertTrue(calls.isEmpty());
		
		//check STORE
		pm.currentTransaction().commit();
		checkCall(ZooInstanceEvent.PRE_STORE);  //preStore
		checkCall(ZooInstanceEvent.PRE_CLEAR);  
		checkCall(ZooInstanceEvent.POST_CLEAR);  
		checkCall(ZooInstanceEvent.POST_STORE);  //postStore
		assertTrue(calls.isEmpty());
		
		pm.currentTransaction().begin();

		// check LOAD
		assertNotNull(t1.getRef2());
		checkCall(ZooInstanceEvent.LOAD);  
		assertTrue(calls.isEmpty());
		
		// check modify
		t1.setRef2(null);
		checkCall(ZooInstanceEvent.PRE_DIRTY);
		checkCall(ZooInstanceEvent.POST_DIRTY);
		System.err.println("WARNING InstanceLifecycles are currently not correct. " +
				"PRE_DIRTY and POST_DIRTY should be triggered BEFORE/AFTER a field is modified.");
		assertTrue(calls.isEmpty());

		//check refresh-LOAD
		pm.refresh(t1);
		checkCall(ZooInstanceEvent.LOAD);  
		assertTrue(calls.isEmpty());
		
		//check DELETE
		pm.deletePersistent(t1);
		assertTrue(calls.isEmpty());
		pm.currentTransaction().commit();
		checkCall(ZooInstanceEvent.PRE_DELETE);
		checkCall(ZooInstanceEvent.POST_DELETE);
		assertTrue(calls.isEmpty());
		
		TestTools.closePM();
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
		checkCall(ZooInstanceEvent.CREATE);
		assertTrue(calls.isEmpty());
		
		//remove listener
		pm.removeInstanceLifecycleListener(lc);
		
		TestClass t2 = new TestClass();
		pm.makePersistent(t2);
		assertTrue(calls.isEmpty());
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}
	
	private void checkCall(ZooInstanceEvent expected) {
		assertTrue(calls.size() > 0);
		assertEquals(expected, calls.get(0).type);
		assertEquals(TestClass.class, calls.get(0).e.getSource().getClass());
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

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

import static org.junit.Assert.*;

import javax.jdo.JDOHelper;
import javax.jdo.ObjectState;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.api.DBArrayList;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.test.testutil.TestTools;

public class Test_042_TransactionsEvict {

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
	public void testNoEvictNew() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		tc.setInt(5);
		pm.makePersistent(tc);
		
		pm.evictAll();
		pm.evictAll(tc);
		pm.evictAll(false, TestClass.class);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		assertEquals(5, tc.getInt());
		assertTrue(JDOHelper.isPersistent(tc));
		tc.setInt(55);
		assertEquals(55, tc.getInt());
		
		//does commit work?
		pm.deletePersistent(tc);

		pm.currentTransaction().commit();
		pm.close();
	}
	
	@Test
	public void testNoEvict() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		tc.setInt(55);
		pm.makePersistent(tc);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		
		pm.evictAll();
		pm.evictAll(tc);
		pm.evictAll(true, TestClass.class);
		
		assertEquals(55, tc.getInt());
		tc.setInt(555);
		assertEquals(555, tc.getInt());
		assertTrue(JDOHelper.isPersistent(tc));
		
		//does commit work?
		pm.deletePersistent(tc);

		pm.currentTransaction().commit();
		pm.close();
	}
	
	
	@Test
	public void testSingleEvict() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		tc.setInt(55);
		pm.makePersistent(tc);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		pm.evict(tc);
		assertEquals(ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL, JDOHelper.getObjectState(tc));
		
		assertEquals(55, tc.getInt());
		tc.setInt(555);
		assertEquals(555, tc.getInt());
		assertTrue(JDOHelper.isPersistent(tc));
		
		//does commit work?
		pm.deletePersistent(tc);

		pm.currentTransaction().commit();
		pm.close();
	}
	
	@Test
	public void testSingleEvictAll() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		tc.setInt(55);
		pm.makePersistent(tc);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//evicted by commit
		assertEquals(ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL, JDOHelper.getObjectState(tc));
		pm.refresh(tc);
		
		//should have no effect (super-class)
		pm.evictAll(false, PersistenceCapableImpl.class);
		pm.evictAll(true, DBArrayList.class);
		assertEquals(ObjectState.PERSISTENT_CLEAN, JDOHelper.getObjectState(tc));

		//test super-class + true
		pm.evictAll(true, PersistenceCapableImpl.class);
		assertEquals(ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL, JDOHelper.getObjectState(tc));
		
		//test false
		pm.refresh(tc);
		assertEquals(ObjectState.PERSISTENT_CLEAN, JDOHelper.getObjectState(tc));
		pm.evictAll(false, TestClass.class);
		assertEquals(ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL, JDOHelper.getObjectState(tc));

		//test true
		pm.refresh(tc);
		assertEquals(ObjectState.PERSISTENT_CLEAN, JDOHelper.getObjectState(tc));
		pm.evictAll(true, TestClass.class);
		assertEquals(ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL, JDOHelper.getObjectState(tc));

		pm.currentTransaction().commit();
		pm.close();
	}
	
	@Test
	public void testEvictAttributes() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		tc.setInt(55);
		tc.setRef1(tc);  //ref to self
		pm.makePersistent(tc);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		assertEquals(tc, tc.getRef1());
		assertEquals(tc, TestTools.getFieldValue("_ref1", tc));
		assertEquals(55, TestTools.getFieldValue("_int", tc));
		pm.evict(tc);
		assertEquals(null, TestTools.getFieldValue("_ref1", tc));
		//primitive are not evicted
		assertEquals(55, TestTools.getFieldValue("_int", tc));
		assertEquals(tc, tc.getRef1());

		pm.currentTransaction().commit();
		pm.close();
	}
	
	/**
	 * That that primitive eviction works if enforced.
	 */
	@Test
	public void testEvictPrimitives() {
		ZooJdoProperties props = TestTools.getProps();
		props.setZooEvictPrimitives(true);
		PersistenceManager pm = TestTools.openPM(props);
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		tc.setInt(55);
		tc.setRef1(tc);  //ref to self
		pm.makePersistent(tc);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		assertEquals(tc, tc.getRef1());
		assertEquals(tc, TestTools.getFieldValue("_ref1", tc));
		assertEquals(55, TestTools.getFieldValue("_int", tc));
		pm.evict(tc);
		assertEquals(null, TestTools.getFieldValue("_ref1", tc));
		//primitive should =now be gone as well
		assertEquals(0, TestTools.getFieldValue("_int", tc));
		assertEquals(tc, tc.getRef1());

		pm.currentTransaction().commit();
		pm.close();
	}
	
	/**
	 * That that values are not retained (default).
	 */
	@Test
	public void testRetainValuesFalse() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		tc.setInt(55);
		tc.setRef1(tc);  //ref to self
		pm.makePersistent(tc);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		assertEquals(null, TestTools.getFieldValue("_ref1", tc));
		//still 55, because evict is by default=false
		assertEquals(55, TestTools.getFieldValue("_int", tc));

		pm.currentTransaction().commit();
		pm.close();
	}
	
	/**
	 * That that values are retained if so wished.
	 */
	@Test
	public void testRetainValues() {
		ZooJdoProperties props = TestTools.getProps();
		props.setRetainValues(true);
		PersistenceManager pm = TestTools.openPM(props);
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		tc.setInt(55);
		tc.setRef1(tc);  //ref to self
		pm.makePersistent(tc);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		assertEquals(tc, TestTools.getFieldValue("_ref1", tc));
		assertEquals(55, TestTools.getFieldValue("_int", tc));

		pm.currentTransaction().commit();
		pm.close();
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

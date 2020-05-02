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

import java.util.Collection;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.test.testutil.TestTools;

public class Test_074_QueryComplex {

	@BeforeClass
	public static void setUp() {
        TestTools.removeDb();
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class, TestClassTiny.class, TestClassTiny2.class);
	}

	/**
	 * Both(!) queries below used to return all objects for which (_bool==true) was true.
	 */
	@Test
	public void testExclusiveAnd() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		TestClass tc1 = new TestClass();
		tc1.setData(1, true, 'c', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2}, 
				-1.1f, 35);
		pm.makePersistent(tc1);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		Query q = pm.newQuery(TestClass.class, "_bool == false && _bool == true");
		Collection<?> c = (Collection<?>) q.execute();
		assertEquals(0, c.size());

		Query q2 = pm.newQuery(TestClass.class, "_bool == true && _bool == false");
		Collection<?> c2 = (Collection<?>) q2.execute();
		assertEquals(0, c2.size());

		pm.deletePersistent(tc1);
		pm.currentTransaction().commit();
		
		TestTools.closePM();
	}

	
	@Test
	public void testDeletePersisistentAll() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		TestClass tc1 = new TestClass();
		tc1.setData(1, true, 'c', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
				-1.1f, 35);
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(12, true, 'd', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
				-1.0f, 3);
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(123, true, 'e', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
				-0.1f, 0.35);
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(1234, true, 'f', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
				1.1f, -35);
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(12345, true, 'g', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
				11.1f, 335);
		pm.makePersistent(tc1);
		
		pm.currentTransaction().commit();
		TestTools.closePM();


		//check delete operation
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		Query q = pm.newQuery(TestClass.class, "_bool == true && _bool == false");
		assertEquals(0, q.deletePersistentAll());
		
		//TODO this should also work without commit!
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		q = pm.newQuery(TestClass.class, "_bool == true");
		assertEquals(5, q.deletePersistentAll());

		//test before committing changes
        //should return 5 for IgnoreCache=true
        pm.setIgnoreCache(true);
        q = pm.newQuery(TestClass.class, "_bool == true");
        q.setIgnoreCache(true);
        assertEquals(5, q.deletePersistentAll());

        //should return 0 for IgnoreCache=false
        pm.setIgnoreCache(false);
        q = pm.newQuery(TestClass.class, "_bool == true");
        assertEquals(0, q.deletePersistentAll());

        //set back to default
        pm.setIgnoreCache(pm.getPersistenceManagerFactory().getIgnoreCache());
        
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//now test after committing changes
		q = pm.newQuery(TestClass.class, "_bool == true");
		assertEquals(0, q.deletePersistentAll());

		TestTools.closePM();
		
		//TODO improve:
		//- test that also non-committed objects in cache a flagged as deleted
		//- test that committed objects are only deleted if the modified client version matches
		//  the query.
	}
	
	@Test
	public void testQueryWhereSuperClassDoesNotContainAttribute() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClassTiny t1 = new TestClassTiny();
		pm.makePersistent(t1);
		
		TestClassTiny2 t2 = new TestClassTiny2();
		pm.makePersistent(t2);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		//now query for i2.
        String filter = "this.i2 == param";
        Query query = pm.newQuery(pm.getExtent(TestClassTiny2.class,true), filter);
        query.declareParameters("int param");
        Collection<?> c = (Collection<?>) query.execute(0);
        assertEquals(1, c.size());

        TestTools.closePM();
	}
	
	@Test
	public void testExcludingSubClass() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClassTiny t1 = new TestClassTiny();
		pm.makePersistent(t1);
		
		TestClassTiny2 t2 = new TestClassTiny2();
		pm.makePersistent(t2);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		//now query for _int
		//with subs
        String filter = "this._int == param";
        Query query = pm.newQuery(pm.getExtent(TestClassTiny.class, true), filter);
        query.declareParameters("int param");
        Collection<?> c = (Collection<?>) query.execute(0);
        assertEquals(2, c.size());

        //without subs
        filter = "this._int == param";
        query = pm.newQuery(pm.getExtent(TestClassTiny.class, false), filter);
        query.declareParameters("int param");
        c = (Collection<?>) query.execute(0);
        assertEquals(1, c.size());

        TestTools.closePM();
	}
	
	@Test
	public void testExcludingSubClassWithIndex() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		ZooJdoHelper.createIndex(pm, TestClassTiny.class, "_int", false);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		TestClassTiny t1 = new TestClassTiny();
		pm.makePersistent(t1);
		
		TestClassTiny2 t2 = new TestClassTiny2();
		pm.makePersistent(t2);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		//now query for _int
		//with subs
        String filter = "this._int == param";
        Query query = pm.newQuery(pm.getExtent(TestClassTiny.class, true), filter);
        query.declareParameters("int param");
        Collection<?> c = (Collection<?>) query.execute(0);
        assertEquals(2, c.size());

        //without subs
        filter = "this._int == param";
        query = pm.newQuery(pm.getExtent(TestClassTiny.class, false), filter);
        query.declareParameters("int param");
        c = (Collection<?>) query.execute(0);
        assertEquals(1, c.size());

        TestTools.closePM();
	}
	
	@After
	public void afterTest() {
		TestTools.closePM();

		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		pm.newQuery(TestClassTiny2.class).deletePersistentAll();
		pm.newQuery(TestClassTiny.class).deletePersistentAll();
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
	@AfterClass
	public static void tearDown() {
	    TestTools.closePM();
		TestTools.removeDb();
	}
	
}

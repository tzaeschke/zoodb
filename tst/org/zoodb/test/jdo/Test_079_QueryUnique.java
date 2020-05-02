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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.test.testutil.TestTools;

/**
 * Tests for query parameters.
 * 
 * PARAMETERS:
 * - parameters can be declared implicitly by a prefixing ':' or explicitly using
 *   the PARAMETERS keyword or declareParameters();
 * - They can be left-hand and right-hand side.
 * 
 * 
 * @author ztilmann
 *
 */
public class Test_079_QueryUnique {

	@BeforeClass
	public static void setUp() {
        TestTools.removeDb();
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
	}

	@Before
	public void before() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        pm.newQuery(TestClass.class).deletePersistentAll();
        
        TestClass tc1 = new TestClass();
        tc1.setData(1, false, 'c', (byte)127, (short)32001, 1234567890L, "xyz", new byte[]{1,2},
        		-1.1f, 35);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(12, false, 'd', (byte)126, (short)32002, 1234567890L, "xyz", new byte[]{1,2},
        		-0.1f, 34);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(123, false, 'e', (byte)125, (short)32003, 1234567890L, "xyz", new byte[]{1,2},
        		0.1f, 3.0);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(1234, false, 'f', (byte)124, (short)32004, 1234567890L, "xyz", new byte[]{1,2},
        		1.1f, -0.01);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(12345, false, 'g', (byte)123, (short)32005, 1234567890L, "xyz", new byte[]{1,2},
        		11.1f, -35);
        pm.makePersistent(tc1);
        
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

	@Test
	public void testSetUnique() {
		internalTestSetUnique(TYPE.CLASS_QUERY);
		internalTestSetUnique(TYPE.WHERE_QUERY);
		internalTestSetUnique(TYPE.SET_FILTER);
	}
	
	private void internalTestSetUnique(TYPE type) {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		Object o;
		
		q = newQuery(pm, "_int == 12", type);
		q.setUnique(true);
		o = q.execute();
		assertNotNull(o);
		assertTrue(o instanceof TestClass);

		q = newQuery(pm, "_int == 1234567", type);
		q.setUnique(true);
		o = q.execute();
		assertNull(o);
		
		q = newQuery(pm, "_int >= 1234", type);
		q.setUnique(true);
		try {
			o = q.execute();
			fail();
		} catch (JDOUserException e) {
			//too many results
		}
		
		TestTools.closePM();
	}
	
	/**
	 * Test the no-filter code path. 
	 */
	@Test
	public void testUniqueResultType() {
		//populate with schema and single instance
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooJdoHelper.schema(pm).addClass(TestClassTiny.class);
		ZooJdoHelper.schema(pm).addClass(TestClassTiny2.class);
		pm.makePersistent(new TestClassTiny());
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		internalTestUniqueNoFilter(TYPE.CLASS_QUERY);
		internalTestUniqueNoFilter(TYPE.WHERE_QUERY);
		internalTestUniqueNoFilter(TYPE.SET_FILTER);
	}
	
	private void internalTestUniqueNoFilter(TYPE type) {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		Object o;
		
		q = newQuery(pm, type, TestClassTiny.class);
		q.setUnique(true);
		o = q.execute();
		assertNotNull(o);
		assertTrue(o instanceof TestClassTiny);

		q = newQuery(pm, type, TestClassTiny2.class);
		q.setUnique(true);
		o = q.execute();
		assertNull(o);
		
		q = newQuery(pm, type, TestClass.class);
		q.setUnique(true);
		try {
			o = q.execute();
			fail();
		} catch (JDOUserException e) {
			//too many results
		}
		
		TestTools.closePM();
	}
	
	@Test
	public void testUNIQUE() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		String prefix = "SELECT UNIQUE FROM " + TestClass.class.getName() + " WHERE ";
		
		Query q = null; 
		Object o;
		
		q = pm.newQuery(prefix + "_int == 12");
		o = q.execute();
		assertNotNull(o);
		assertTrue(o instanceof TestClass);

		q = pm.newQuery(prefix + "_int == 1234567");
		o = q.execute();
		assertNull(o);
		
		q = pm.newQuery(prefix + "_int >= 1234");
		try {
			o = q.execute();
			fail();
		} catch (JDOUserException e) {
			//too many results
		}
		
		TestTools.closePM();
	}
		
	
	private enum TYPE {
		SET_FILTER,
		CLASS_QUERY,
		WHERE_QUERY
	}
	
	private Query newQuery(PersistenceManager pm, String str, TYPE type) {
		switch (type) {
		case SET_FILTER:
			Query q = pm.newQuery(TestClass.class);
			q.setFilter(str);
			return q;
		case CLASS_QUERY: 
			return pm.newQuery(TestClass.class, str);
		case WHERE_QUERY:
			return pm.newQuery("SELECT FROM " + TestClass.class.getName() + " WHERE " + str);
		default: throw new IllegalArgumentException();
		}
	}
	
	private Query newQuery(PersistenceManager pm, TYPE type, Class<?> cls) {
		switch (type) {
		case SET_FILTER:
			return pm.newQuery(cls);
		case CLASS_QUERY: 
			return pm.newQuery(cls);
		case WHERE_QUERY:
			return pm.newQuery("SELECT FROM " + cls.getName());
		default: throw new IllegalArgumentException();
		}
	}
}

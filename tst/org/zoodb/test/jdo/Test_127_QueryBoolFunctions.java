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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;

import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.testutil.TestTools;

/**
 * Tests for query setOrdering().
 * 
 * @author ztilmann
 *
 */
public class Test_127_QueryBoolFunctions {

	@BeforeClass
	public static void setUp() {
        TestTools.removeDb();
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
    	TestTools.defineSchema(TestQueryClass.class);
	}

	@Before
	public void before() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        pm.newQuery(TestClass.class).deletePersistentAll();
        
        TestClass tc1 = new TestClass();
        tc1.setData(1, false, 'c', (byte)127, (short)32001, 1234567890L, "xyz5", new byte[]{1,2},
        		-1.1f, 35);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(12, false, 'd', (byte)126, (short)32002, 1234567890L, "xyz4", new byte[]{1,2},
        		-0.1f, 34);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(123, false, 'x', (byte)125, (short)32003, 1234567891L, "xyz1", new byte[]{1,2},
        		0.1f, 3.0);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(1234, false, 'f', (byte)124, (short)32004, 1234567890L, "xyz2", new byte[]{1,2},
        		1.1f, -0.01);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(12345, false, 'g', (byte)123, (short)32005, 1234567890L, "xyz3", new byte[]{1,2},
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
	public void testBoolFunctionFail() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		checkSetFilterFails(pm, "isEmpty");
		//checkSetFilterFails(pm, "isEmpty == 3");
		checkSetFilterFails(pm, "isEmpty()");
		
		checkSetFilterFails(pm, "startsWith('asc')");
		
		checkSetFilterFails(pm, "_int.isEmpty()");

		checkSetFilterFails(pm, "_string.isEmpty()");

		checkSetFilterFails(pm, "_string.startsWith");
		checkSetFilterFails(pm, "_string.startsWith()");
		checkSetFilterFails(pm, "_string.startsWith(1)");
		checkSetFilterFails(pm, "_string.startsWith('z', 'b')");
		checkSetFilterFails(pm, "_string.startsWith('z').startsWith('x')");
		
		TestTools.closePM();
	}
	
	private void checkSetFilterFails(PersistenceManager pm, String s) {
		Query q1 = pm.newQuery(TestClass.class);
		try {
			q1.setFilter(s);
			q1.compile();
			fail();
		} catch (JDOUserException e) {
			//good, we got an JDOUSerException()
		}
		
		try {
			Query q2 = pm.newQuery(TestClass.class, s);
			q2.compile();
			fail();
		} catch (JDOUserException e) {
			//good, we got an JDOUSerException()
		}
	}
	
	@Test
	public void testString() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("_string.matches('xyz1')");
		checkString(q, "xyz1");

		q.setFilter("_string.matches('xyz')");
		checkString(q);

		q.setFilter("_string.matches('.*3.*')");
		checkString(q, "xyz3");

		q.setFilter("_string.matches('.*y.*')");
		checkString(q, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		q.setFilter("_string.startsWith('xyz1')");
		checkString(q, "xyz1");

		q.setFilter("_string.startsWith('xyz')");
		checkString(q, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		q.setFilter("_string.startsWith('xyz12')");
		checkString(q);

		q.setFilter("_string.endsWith('xyz1')");
		checkString(q, "xyz1");

		q.setFilter("_string.endsWith('yz1')");
		checkString(q, "xyz1");

		q.setFilter("_string.endsWith('xyz12')");
		checkString(q);

		//non-JDO:
		q.setFilter("_string.contains('xyz1')");
		checkString(q, "xyz1");

		q.setFilter("_string.contains('yz1')");
		checkString(q, "xyz1");

		q.setFilter("_string.contains('xyz12')");
		checkString(q);

		q.setFilter("_string.contains('xyz')");
		checkString(q, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		TestTools.closePM();
	}
	
	@Test
	public void testStringWithIndex() {
		TestTools.defineIndex(TestClass.class, "_string", true);
		testString();
	}
	
    @SuppressWarnings("unchecked")
	private void checkString(Query q, String ... matches) {
    	Collection<TestClass> c = (Collection<TestClass>) q.execute(); 
		for (int i = 0; i < matches.length; i++) {
			boolean match = false;
			for (TestClass t: c) {
				if (t.getString().equals(matches[i])) {
					match = true;
					break;
				}
			}
			assertTrue(match);
		}
		assertEquals(matches.length, c.size());
	}

    @SuppressWarnings("unchecked")
	private void checkString(Query q, Object param1, String ... matches) {
    	Collection<TestClass> c = (Collection<TestClass>) q.execute(param1); 
		for (int i = 0; i < matches.length; i++) {
			boolean match = false;
			for (TestClass t: c) {
				if (t.getString().equals(matches[i])) {
					match = true;
					break;
				}
			}
			assertTrue(match);
		}
		assertEquals(matches.length, c.size());
	}

	
    @Test
    public void testList() {
    	populateTQC();
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("listObj.isEmpty()");
		checkString(q, "0000", "NULL");

		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("listObj.contains(1234)");
		checkString(q, "1111");

		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("listObj.contains(1234L)");
		checkString(q);

		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("listObj.contains(123)");
		checkString(q);
   }
	
    @Test
    public void testListTC() {
    	Object oid1 = populateTQC();
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("listTC.isEmpty()");
		checkString(q, "0000", "NULL");

		Object o1 = pm.getObjectById(oid1);
		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("listTC.contains(:o1)");
		checkString(q, o1, "1111");
   }
	
    @Test
    public void testCollection() {
    	populateTQC();
  		PersistenceManager pm = TestTools.openPM();
  		pm.currentTransaction().begin();

  		Query q = null; 

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("coll.isEmpty()");
  		checkString(q, "0000", "NULL");

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("coll.contains('coll')");
  		checkString(q, "1111");

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("coll.contains(null)");
  		checkString(q);

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("listObj.contains('123')");
  		checkString(q);
  		
  		TestTools.closePM();
    }
	
    @Test
    public void testMap() {
    	populateTQC();
  		PersistenceManager pm = TestTools.openPM();
  		pm.currentTransaction().begin();

  		Query q = null; 
  		
  		//See spec 14.6.2: map.isEmpty() on 'map=null' return true
  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("map.isEmpty()");
  		checkString(q, "0000", "NULL");

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("map.containsKey('key')");
  		checkString(q, "1111");

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("map.containsKey(null)");
  		checkString(q);

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("map.containsKey('123')");
  		checkString(q);
  		
  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("map.containsValue(_ref2)");
  		checkString(q, "1111");

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("map.containsValue(null)");
  		checkString(q);

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("map.containsValue('123')");
  		checkString(q);
  		
  		TestTools.closePM();
    }
    
    private Object populateTQC() {
  		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		//nulls
		TestQueryClass tN = new TestQueryClass(); 
		tN.setString("NULL");
		pm.makePersistent(tN);

		//empty list
		TestQueryClass t1 = new TestQueryClass();
		t1.init();
		t1.setString("0000");
		pm.makePersistent(t1);
		Object oid1 = JDOHelper.getObjectId(t1);
		
		//list
		TestQueryClass t2 = new TestQueryClass();
		t2.init();
		t2.setString("1111");
		t2.addInt(123);
		t2.addObj(Integer.valueOf(1234));
		t2.addTC(t1);
		t2.addToMap("key", t1);
		t2.addToSet("123");
		t2.addToColl("coll");
		t2.setRef2(t1);
		pm.makePersistent(t2);
		
		pm.currentTransaction().commit();
    	TestTools.closePM();
    	return oid1;
    }
	
}

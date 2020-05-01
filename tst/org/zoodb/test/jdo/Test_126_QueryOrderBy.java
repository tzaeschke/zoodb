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
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

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
public class Test_126_QueryOrderBy {

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
        TestTools.closePM();;
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
	public void testFailuresOrderBy() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestClass.class);
		q.setOrdering(null);;
		q.execute();

		q.setOrdering("");
		q.execute();
		
		q.setOrdering(" ");
		q.execute();
		
		checkSetOrderingFails(pm, "xyz");
		
		checkSetOrderingFails(pm, "asc");
		checkSetOrderingFails(pm, "desc");
		
		checkSetOrderingFails(pm, "xyz asc");
		checkSetOrderingFails(pm, "xyz desc");

		checkSetOrderingFails(pm, "_int ascxyz");
		checkSetOrderingFails(pm, "_int descxyz");

		checkSetOrderingFails(pm, "_object asc");
		checkSetOrderingFails(pm, "_object desc");
		checkSetOrderingFails(pm, "_bArray asc");
		checkSetOrderingFails(pm, "_bArray desc");

		checkSetOrderingFails(pm, "_int");
		checkSetOrderingFails(pm, "_string");
		
		checkSetOrderingFails(pm, "_int asc, ");
		
		checkSetOrderingFails(pm, ", _int asc");
		
		checkSetOrderingFails(pm, " , ");
		
		checkSetOrderingFails(pm, "_int asc, _long");
		checkSetOrderingFails(pm, "_long, _int asc");
		checkSetOrderingFails(pm, "_int asc, asc");
		checkSetOrderingFails(pm, "asc, _int asc");
		
		checkSetOrderingFails(pm, "_int asc _int asc");

		//semantic collision...
		checkSetOrderingFails(pm, "_int asc, _int asc");
	}
	
	private void checkSetOrderingFails(PersistenceManager pm, String s) {
		Query q1 = pm.newQuery(TestClass.class);
		try {
			q1.setOrdering(s);
			q1.compile();
			fail();
		} catch (JDOUserException e) {
			//good, we got an JDOUSerException()
		}
		
		try {
			Query q2 = pm.newQuery(TestClass.class, "order by " + s);
			q2.compile();
			fail();
		} catch (JDOUserException e) {
			//good, we got an JDOUSerException()
		}
	}
	
    @Test
    public void testAsc() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		//try internal extent
		q = pm.newQuery(TestClass.class);
		q.setOrdering("_byte    asc");
		checkOrder(q, 123, 124, 125, 126, 127);
		
		q.setOrdering("  _int asc  ");
		checkOrder(q, 127, 126, 125, 124, 123);
		
		q.setOrdering("_string asc");
		checkOrder(q, 125, 124, 123, 126, 127);
		
		q.setOrdering("_char asc");
		checkOrder(q, 127, 126, 124, 123, 125);
		
		q.setOrdering("_double asc");
		checkOrder(q, 123, 124, 125, 126, 127);
		
		q.setOrdering("_float asc");
		checkOrder(q, 127, 126, 125, 124, 123);
		
		q.setOrdering("_float ascending");
		checkOrder(q, 127, 126, 125, 124, 123);
		
		q.setOrdering("_float ASC");
		checkOrder(q, 127, 126, 125, 124, 123);
		
		q.setOrdering("_float ASCENDING");
		checkOrder(q, 127, 126, 125, 124, 123);
		
		TestTools.closePM();
    }

    @Test
    public void testDesc() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		//try internal extent
		q = pm.newQuery(TestClass.class);
		q.setOrdering("_byte   	 desc");
		checkOrder(q, 127, 126, 125, 124, 123);
		
		q.setOrdering("  _int desc  ");
		checkOrder(q, 123, 124, 125, 126, 127);
		
		q.setOrdering("_string desc");
		checkOrder(q, 127, 126, 123, 124, 125);
		
		q.setOrdering("_char desc");
		checkOrder(q, 125, 123, 124, 126, 127);
		
		q.setOrdering("_double desc");
		checkOrder(q, 127, 126, 125, 124, 123);
		
		q.setOrdering("_float desc");
		checkOrder(q, 123, 124, 125, 126, 127);
		
		q.setOrdering("_float descending");
		checkOrder(q, 123, 124, 125, 126, 127);
		
		q.setOrdering("_float DESC");
		checkOrder(q, 123, 124, 125, 126, 127);
		
		q.setOrdering("_float DESCENDING");
		checkOrder(q, 123, 124, 125, 126, 127);
		
		TestTools.closePM();
    }

    @SuppressWarnings("unchecked")
	private void checkOrder(Query q, int ... order) {
    	Collection<TestClass> c = (Collection<TestClass>) q.execute(); 
    	List<TestClass> oa = new ArrayList<>(c);
		for (int i = 0; i < order.length; i++) {
			assertEquals(order[i], oa.get(i).getByte());
		}
		assertEquals(order.length, c.size());
	}

	
    /**
     * Test 'null', multi-sort and string-only query.
     */ 
    @Test
    public void testMultiSorting() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		for (int i = 0; i < 6; i++) {
			TestClass t = new TestClass();
	        t.setData(-i, false, 'x', (byte) (100+(i%3)), 
	        		(short)32003, 1234567890L + i%5, null, null,
	        		0.1f, 3.0);
	        pm.makePersistent(t);
		}
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		Query q = pm.newQuery("select from " + TestClass.class.getName() + " where _int < 1 " +
				"order by _string asc, _byte desc, _int asc");
		
		Collection<TestClass> c = (Collection<TestClass>) q.execute();
		Iterator<TestClass> it = c.iterator();
		TestClass t;
		t = it.next();
		assertEquals(null, t.getString());
		assertEquals(102, t.getByte());
		assertEquals(-5, t.getInt());

		t = it.next();
		assertEquals(null, t.getString());
		assertEquals(102, t.getByte());
		assertEquals(-2, t.getInt());

		t = it.next();
		assertEquals(null, t.getString());
		assertEquals(101, t.getByte());
		assertEquals(-4, t.getInt());

		t = it.next();
		assertEquals(null, t.getString());
		assertEquals(101, t.getByte());
		assertEquals(-1, t.getInt());

		t = it.next();
		assertEquals(null, t.getString());
		assertEquals(100, t.getByte());
		assertEquals(-3, t.getInt());

		t = it.next();
		assertEquals(null, t.getString());
		assertEquals(100, t.getByte());
		assertEquals(-0, t.getInt());

		
		TestTools.closePM();
    }
	
}

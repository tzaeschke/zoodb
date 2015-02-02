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
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Iterator;

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
			q1.execute();
			fail();
		} catch (JDOUserException e) {
			//good, we got an JDOUSerException()
		}
		
		try {
			Query q2 = pm.newQuery(TestClass.class, "order by " + s);
			q2.execute();
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
		
		TestTools.closePM();
    }

    @SuppressWarnings("unchecked")
	private void checkOrder(Query q, int ... order) {
    	Collection<TestClass> c = (Collection<TestClass>) q.execute(); 
    	TestClass[] oa = c.toArray(new TestClass[order.length]);
		for (int i = 0; i < order.length; i++) {
			assertEquals(order[i], oa[i].getByte());
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
	
    @Test
    public void testAscWithIndex() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		//TODO test that index is used for asc/desc 
		System.err.println("TODO Test_126.testAscWithIndex()");

		fail();
		
		TestTools.closePM();
    }
	
    @Test
    public void testDescWithIndex() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		//TODO test that index is used for asc/desc 
		System.err.println("TODO Test_126.testDescWithIndex()");

		fail();
		
		TestTools.closePM();
    }
	
}

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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.jdo.TestClass.ENUM;
import org.zoodb.test.testutil.TestTools;

/**
 * Tests for query setOrdering().
 * 
 * @author ztilmann
 *
 */
public class Test_170_QuerySetRange {

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
        		-1.1f, 35, null);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(12, false, 'd', (byte)126, (short)32002, 1234567890L, "xyz4", new byte[]{1,2},
        		-0.1f, 34, ENUM.E);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(123, false, 'x', (byte)125, (short)32003, 1234567891L, "xyz1", new byte[]{1,2},
        		0.1f, 3.0, ENUM.A);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(1234, false, 'f', (byte)124, (short)32004, 1234567890L, "xyz2", new byte[]{1,2},
        		1.1f, -0.01, ENUM.B);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(12345, false, 'g', (byte)123, (short)32005, 1234567890L, "xyz3", new byte[]{1,2},
        		11.1f, -35, ENUM.C);
        pm.makePersistent(tc1);
        
        tc1.setIntObj(tc1.getInt());
        
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
	public void testRangeParseFail() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		checkSetFilterFails(pm, "RANGE");
		checkSetFilterFails(pm, "RANGE 3");
		checkSetFilterFails(pm, "RANGE 3, ");
		
		checkSetFilterFails(pm, "range 3,2");
		
		checkSetFilterFails(pm, "range -1, 5");

		checkSetFilterFails(pm, "range 3, 4.7");

		checkSetFilterFails(pm, "range range 3, 4");
		checkSetFilterFails(pm, "RaNge 3, 4");
		
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
	public void testRangeIntFunctionFail() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		q = pm.newQuery(TestClass.class);

		try {
			q.setRange(-1, 3);
			fail();
		} catch (JDOUserException e) {
			//good
		}
		
		try {
			q.setRange(3, 1);
			fail();
		} catch (JDOUserException e) {
			//good
		}

		TestTools.closePM();
	}
	
	
	@Test
	public void testRangeStrFunctionFail() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		checkSetRangeFails(pm, "-1, 3");
		checkSetRangeFails(pm, "3, 1");

		TestTools.closePM();
	}
	
	private void checkSetRangeFails(PersistenceManager pm, String s) {
		Query q1 = pm.newQuery(TestClass.class);
		try {
			q1.setRange(s);
			q1.compile();
			fail();
		} catch (JDOUserException e) {
			//good, we got an JDOUSerException()
		}
		
		try {
			Query q2 = pm.newQuery(TestClass.class, "RANGE " + s);
			q2.compile();
			fail();
		} catch (JDOUserException e) {
			//good, we got an JDOUSerException()
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRangeWithParams() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = pm.newQuery(TestClass.class);
		
		List<TestClass> c0 = (List<TestClass>) q.execute();
		List<TestClass> a0 = new ArrayList<>(c0);

		q.setRange(":min, :max");

		List<TestClass> c2 = (List<TestClass>) q.execute(0, 5);
		List<TestClass> a2 = new ArrayList<>(c2);
		assertEquals(a0, a2);

		List<TestClass> c3 = (List<TestClass>) q.execute(1, 4);
		List<TestClass> a3 = new ArrayList<>(c3);
		for (int i = 1; i <= 3; i++) {
			assertEquals(a0.get(i).getString(), a3.get(i-1).getString());
		}
		
		TestTools.closePM();
	}
	

	@Test
	public void testString() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("_string.indexOf('yz1') == 1");
		checkString(q, 0, 3, "xyz1");

		q.setFilter("_string.indexOf('xz') >= 0");
		checkString(q, 0, 3);

		q.setFilter("_string.indexOf('3', 3) > -1");
		checkString(q, 0, 3, "xyz3");

		q.setFilter("_string.indexOf('y') == 1");
		checkString(q, 1, 3, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		q.setFilter("_string.substring(2) == 'z1')");
		checkString(q, 1, 3, "xyz1");

		q.setFilter("_string.substring(1,3) == 'yz')");
		checkString(q, 1, 3, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		q.setFilter("_string.substring(0,3).startsWith('xyz12')");
		checkString(q, 0, 7);

		q.setFilter("_string.toLowerCase().endsWith('xyz1')");
		checkString(q, 1, 3, "xyz1");

		q.setFilter("_string.toUpperCase().toLowerCase().endsWith('xyz1')");
		checkString(q, 1, 3, "xyz1");

		q.setFilter("_string.toLowerCase() == 'xyz1'");
		checkString(q, 1, 3, "xyz1");

		q.setFilter("_string.toUpperCase() == 'xyz1'");
		checkString(q, 3, 9);

		q.setFilter("_string.toUpperCase() == 'XYZ1'");
		checkString(q, 1, 3, "xyz1");

		q.setFilter("_string.substring(1,2).toUpperCase() == 'Y'");
		checkString(q, 1, 3, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		q.setFilter("'Hello'.substring(0,1).toUpperCase() == 'H'");
		checkString(q, 1, 3, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		q.setFilter("'1234'.substring(0,1).toLowerCase() == _string.substring(3,4)");
		checkString(q, 1, 3, "xyz1");

		q.setFilter("_string == '  xyz1 '.trim()");
		checkString(q, 1, 3, "xyz1");

		q.setFilter("_string.trim() == 'xyz1'");
		checkString(q, 1, 3, "xyz1");

		q.setFilter("_string.length() == 4");
		checkString(q, 1, 3, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		q.setFilter("_string.length() >= 'xyz1'.length()");
		checkString(q, 1, 3, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		q.setFilter("_string.length() == ' xyz1  '.trim().length()");
		checkString(q, 1, 3, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		TestTools.closePM();
	}
	
	@Test
	public void testStringWithIndex() {
		TestTools.defineIndex(TestClass.class, "_string", true);
		testString();
	}
	
	@Test
	public void testEnum() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("_enum.toString() == 'A'");
		checkString(q, 0, 1, "xyz1");

		q.setFilter("_enum.toString().substring(0) == 'A'");
		checkString(q, 1, 2, "xyz1");

		q.setFilter("_enum.ordinal() == 2");
		checkString(q, 1, 1, "xyz3");

		TestTools.closePM();
	}

    @SuppressWarnings("unchecked")
	private void checkString(Query q, int i1, int i2, String ... matches) {
    	q.setRange(0, Long.MAX_VALUE);
    	List<TestClass> c = new ArrayList<>((List<TestClass>) q.execute());
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
		
		//now assert subset with setRange(long, long)
		q.setRange(i1, i2);
		List<TestClass> c2 = new ArrayList<>((List<TestClass>) q.execute());
		Iterator<TestClass> iter2 = c2.iterator();
	   	int pos2 = 0;
		for (TestClass t: c) {
			if (pos2 >= i1 && pos2 < i2) {
				TestClass t2 = iter2.next();
				assertEquals(t.getString(), t2.getString());
			}
			pos2++;
		}
   		
   		if (i1 >= c.size()) {
   			assertEquals(0, c2.size());
   		} else {
   			if (i2 > c.size()) {
   				i2 = c.size();
   			}
   			assertEquals(i2-i1, c2.size());
   		}
    	q.setRange(0, Long.MAX_VALUE);
		
		//now assert subset with STR
		q.setRange(i1 + ", " + i2);
		List<TestClass> c3 = new ArrayList<>((List<TestClass>) q.execute());
		Iterator<TestClass> iter3 = c3.iterator();
	   	int pos3 = 0;
		for (TestClass t: c) {
			if (pos3 >= i1 && pos3 < i2) {
				TestClass t3 = iter3.next();
				assertEquals(t.getString(), t3.getString());
			}
			pos3++;
		}
   		
   		if (i1 >= c.size()) {
   			assertEquals(0, c3.size());
   		} else {
   			if (i2 > c.size()) {
   				i2 = c.size();
   			}
   			assertEquals(i2-i1, c3.size());
   		}
    	q.setRange(0, Long.MAX_VALUE);
    }

    @SuppressWarnings("unchecked")
	private void checkString(Query q, int i1, int i2, Object param1, String ... matches) {
    	q.setRange(0, Long.MAX_VALUE);
    	List<TestClass> c = (List<TestClass>) q.execute(param1); 
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
		
		//now assert subset
		q.setRange(i1, i2);
		List<TestClass> c2 = new ArrayList<>((List<TestClass>) q.execute(param1));
		Iterator<TestClass> iter2 = c2.iterator();
	   	int pos = 0;
		for (TestClass t: c) {
			if (pos >= i1 && pos < i2) {
				TestClass t2 = iter2.next();
				assertEquals(t.getString(), t2.getString());
			}
			pos++;
		}
   		
   		if (i1 >= c.size()) {
   			assertEquals(0, c2.size());
   		} else {
   			if (i2 > c.size()) {
   				i2 = c.size();
   			}
   			assertEquals(i2-i1, c2.size());
   		}
    	q.setRange(0, Long.MAX_VALUE);

		//now assert subset with STR
		q.setRange(i1 + ", " + i2);
		List<TestClass> c3 = new ArrayList<>((List<TestClass>) q.execute(param1));
		Iterator<TestClass> iter3 = c3.iterator();
	   	int pos3 = 0;
		for (TestClass t: c) {
			if (pos3 >= i1 && pos3 < i2) {
				TestClass t3 = iter3.next();
				assertEquals(t.getString(), t3.getString());
			}
			pos3++;
		}
   		
   		if (i1 >= c.size()) {
   			assertEquals(0, c3.size());
   		} else {
   			if (i2 > c.size()) {
   				i2 = c.size();
   			}
   			assertEquals(i2-i1, c3.size());
   		}
    	q.setRange(0, Long.MAX_VALUE);
    }

	
    @Test
    public void testMath() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.abs(_double) > 34f");
		checkString(q, 1, 2, "xyz3", "xyz5");

		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.abs(_double) > 34");
		checkString(q, 1, 2, "xyz3", "xyz5");

		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.sqrt(_float) >= 0");
		checkString(q, 1, 2, "xyz1", "xyz2", "xyz3");

		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.sin(_byte) > 1");
		checkString(q, 1, 2);

		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.cos(_short) < 3");
		checkString(q, 1, 3, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.sin(_long) < 3");
		checkString(q, 1, 3, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.cos(_int) > 3");
		checkString(q, 1, 2);
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.sqrt(_byte) == Math.sqrt(127)");
		checkString(q, 1, 2, "xyz5");
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.sqrt(Math.abs(_float)) == Math.sqrt(Math.abs(_float))");
		checkString(q, 0, 8, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");
		
		//-intObj can be null!
		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.abs(_intObj) > 34");
		checkString(q, 1, 2, "xyz3");

		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.abs(-11) > 34");
		checkString(q, 1, 2);

		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.abs(-50) > 34");
		checkString(q, 1, 2, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.abs(intParam) > 34");
		q.declareParameters("Integer intParam");
		checkString(q, 1, 2, Integer.valueOf(-35), "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.abs(intParam) > 34");
		q.declareParameters("Integer intParam");
		checkString(q, 1, 2, (Integer)null);
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.abs(Math.abs(intParam)) > 34");
		q.declareParameters("Integer intParam");
		checkString(q, 1, 2, (Integer)null);
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.cos(Math.cos(intParam)) > 34");
		q.declareParameters("Integer intParam");
		checkString(q, 1, 2, (Integer)null);
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.sin(Math.sin(intParam)) > 34");
		q.declareParameters("Integer intParam");
		checkString(q, 1, 2, (Integer)null);
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.sqrt(Math.sqrt(intParam)) > 34");
		q.declareParameters("Integer intParam");
		checkString(q, 1, 2, (Integer)null);
		
		TestTools.closePM();
		
		//use refs
    	populateTQC();
  		pm = TestTools.openPM();
  		pm.currentTransaction().begin();

		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("Math.abs(ref.listI.get(0)) == 122");
		checkString(q, 1, 2, "1111");

		TestTools.closePM();
   }
	
    @Test
    public void testList() {
    	populateTQC();
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("listI.get(0) != 122");
		checkString(q, 0, 1, "1111");

		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("listObj.get(0) == 1234)");
		checkString(q, 0, 1, "1111");

		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("listObj.get(0) == 1234L");
		checkString(q, 0, 1, "1111");

		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("listObj.get(1234) == 1234");
		checkString(q, 0, 1);

		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("listObj.get(-1) == 1234");
		checkString(q, 0, 1);

		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("listObj.get(2) == 1234");
		checkString(q, 0, 1);

		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("listObj.size() > 0");
		checkString(q, 0, 1, "1111");

		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("!listObj.isEmpty()");
		checkString(q, 0, 1, "1111");

		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("listObj.contains(1234)");
		checkString(q, 0, 1, "1111");
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
		t1.addInt(122);
		pm.makePersistent(t1);
		Object oid1 = JDOHelper.getObjectId(t1);
		
		//list
		TestQueryClass t2 = new TestQueryClass();
		t2.init();
		t2.setString("1111");
		t2.addInt(123);
		t2.addObj(new Integer(1234));
		t2.addTC(t1);
		t2.addToMap("key", t1);
		t2.addToSet("123");
		t2.addToColl("coll");
		t2.setRef(t1);
		pm.makePersistent(t2);
		
		pm.currentTransaction().commit();
    	TestTools.closePM();
    	return oid1;
    }
	
    @Test
    public void testIndexUsage() {
    	//TODO 
    	//test that setRange() doesn't load objects outside of the range, at least if an index
    	//is used (without index, everything has to be loaded).
    	System.err.println("Implement Test_170.testIndexUsage()");
    }
}

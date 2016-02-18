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
public class Test_129_QueryNonBoolFunctions {

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
	public void testBoolFunctionFail() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		checkSetFilterFails(pm, "indexOf");
		checkSetFilterFails(pm, "indexOf == 3");
		checkSetFilterFails(pm, "indexOf()");
		
		checkSetFilterFails(pm, "indexOf('asc')");
		
		checkSetFilterFails(pm, "_int.indexOf()");

		checkSetFilterFails(pm, "_string.sqrt()");

		checkSetFilterFails(pm, "_string.sqrt");
		checkSetFilterFails(pm, "_string.sqrt()");
		checkSetFilterFails(pm, "_string.sqrt(1)");
		checkSetFilterFails(pm, "_string.sqrt('z', 'b')");
		checkSetFilterFails(pm, "_string.sqrt('z').sqrt('x')");
		
		TestTools.closePM();
	}
	
	private void checkSetFilterFails(PersistenceManager pm, String s) {
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
	public void testString() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("_string.indexOf('yz1') == 1");
		checkString(q, "xyz1");

		q.setFilter("_string.indexOf('xz') >= 0");
		checkString(q);

		q.setFilter("_string.indexOf('3', 3) > -1");
		checkString(q, "xyz3");

		q.setFilter("_string.indexOf('y') == 1");
		checkString(q, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		q.setFilter("_string.substring(2) == 'z1')");
		checkString(q, "xyz1");

		q.setFilter("_string.substring(1,3) == 'yz')");
		checkString(q, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		q.setFilter("_string.substring(0,3).startsWith('xyz12')");
		checkString(q);

		q.setFilter("_string.toLowerCase().endsWith('xyz1')");
		checkString(q, "xyz1");

		q.setFilter("_string.toUpperCase().toLowerCase().endsWith('xyz1')");
		checkString(q, "xyz1");

		q.setFilter("_string.toLowerCase() == 'xyz1'");
		checkString(q, "xyz1");

		q.setFilter("_string.toUpperCase() == 'xyz1'");
		checkString(q);

		q.setFilter("_string.toUpperCase() == 'XYZ1'");
		checkString(q, "xyz1");

		q.setFilter("_string.substring(1,2).toUpperCase() == 'Y'");
		checkString(q, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		q.setFilter("'Hello'.substring(0,1).toUpperCase() == 'H'");
		checkString(q, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		q.setFilter("'1234'.substring(0,1).toLowerCase() == _string.substring(3,4)");
		checkString(q, "xyz1");

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
    public void testMath() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.abs(_double) > 34f");
		checkString(q, "xyz3", "xyz5");

		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.abs(_double) > 34");
		checkString(q, "xyz3", "xyz5");

		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.sqrt(_float) >= 0");
		checkString(q, "xyz1", "xyz2", "xyz3");

		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.sin(_byte) > 1");
		checkString(q);

		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.cos(_short) < 3");
		checkString(q, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.sin(_long) < 3");
		checkString(q, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.cos(_int) > 3");
		checkString(q);
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.sqrt(_byte) == Math.sqrt(127)");
		checkString(q, "xyz5");
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.sqrt(Math.abs(_float)) == Math.sqrt(Math.abs(_float))");
		checkString(q, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");
		
		//-intObj can be null!
		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.abs(_intObj) > 34");
		checkString(q, "xyz3");

		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.abs(-11) > 34");
		checkString(q);

		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.abs(-50) > 34");
		checkString(q, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.abs(intParam) > 34");
		q.declareParameters("Integer intParam");
		checkString(q, Integer.valueOf(-35), "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.abs(intParam) > 34");
		q.declareParameters("Integer intParam");
		checkString(q, (Integer)null);
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.abs(Math.abs(intParam)) > 34");
		q.declareParameters("Integer intParam");
		checkString(q, (Integer)null);
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.cos(Math.cos(intParam)) > 34");
		q.declareParameters("Integer intParam");
		checkString(q, (Integer)null);
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.sin(Math.sin(intParam)) > 34");
		q.declareParameters("Integer intParam");
		checkString(q, (Integer)null);
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("Math.sqrt(Math.sqrt(intParam)) > 34");
		q.declareParameters("Integer intParam");
		checkString(q, (Integer)null);
		
		TestTools.closePM();
		
		//use refs
    	populateTQC();
  		pm = TestTools.openPM();
  		pm.currentTransaction().begin();

		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("Math.abs(ref.listI.get(0)) == 122");
		checkString(q, "1111");

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
		checkString(q, "1111");

		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("listObj.get(0) == 1234)");
		checkString(q, "1111");

		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("listObj.get(0) == 1234L");
		checkString(q);

		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("listObj.get(2) == 1234");
		checkString(q);
   }
	
    @Test
    public void testListTC() {
    	Object oid1 = populateTQC();
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("listTC.get(0) == ref");
		checkString(q, "1111");

		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("listTC.get(123) == ref");
		checkString(q);

		Object o1 = pm.getObjectById(oid1);
		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("listTC.get(0) == :o1");
		checkString(q, o1, "1111");

		//use OID as parameter, TODO doesn't work yet
		//q = pm.newQuery(TestQueryClass.class);
		//q.setFilter("listTC.contains(:oid1)");
		//checkString(q, oid1, "1111");
   }
	
    @Test
    public void testMap() {
    	//TODO this is named 'Map' in Test_127...
    	populateTQC();
  		PersistenceManager pm = TestTools.openPM();
  		pm.currentTransaction().begin();

  		Query q = null; 

  		//remember that nullpointers result in false...
  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("ref.map.isEmpty()");
  		checkString(q, "1111");

 		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("map.get('key') == ref");
  		checkString(q, "1111", "0000");

 		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("map.get('key') != this");
  		checkString(q, "1111", "0000");

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("map.get('key') != null");
  		checkString(q, "1111");

 		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("map.get('key'.toLowercase()) != null");
  		checkString(q, "1111");

 		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("map.size() == 1");
  		checkString(q, "1111");

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
	
}

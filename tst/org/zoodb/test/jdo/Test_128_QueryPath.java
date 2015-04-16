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
public class Test_128_QueryPath {

	private Object oid1;
	private Object oid2;
	private Object oid3;
	private Object oid4;
	private Object oid5;
	
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
        
        TestClass t1 = new TestClass();
        t1.setData(1, false, 'c', (byte)127, (short)32001, 1234567890L, "xyz5", new byte[]{1,2},
        		-1.1f, 35);
        pm.makePersistent(t1);
        TestClass t2 = new TestClass();
        t2.setData(12, false, 'd', (byte)126, (short)32002, 1234567890L, "xyz4", new byte[]{1,2},
        		-0.1f, 34);
        pm.makePersistent(t2);
        TestClass t3 = new TestClass();
        t3.setData(123, false, 'x', (byte)125, (short)32003, 1234567891L, "xyz1", new byte[]{1,2},
        		0.1f, 3.0);
        pm.makePersistent(t3);
        TestClass t4 = new TestClass();
        t4.setData(1234, false, 'f', (byte)124, (short)32004, 1234567890L, "xyz2", new byte[]{1,2},
        		1.1f, -0.01);
        pm.makePersistent(t4);
        TestClass t5 = new TestClass();
        t5.setData(12345, false, 'g', (byte)123, (short)32005, 1234567890L, "xyz3", new byte[]{1,2},
        		11.1f, -35);
        pm.makePersistent(t5);
        
        //large loop
        t1.setRef2(t2);
        t2.setRef2(t3);
        t3.setRef2(t1);
        
        //null
        t4.setRef2(null);
        
        //small loop
        t5.setRef2(t5);
        
        oid1 = pm.getObjectId(t1);
        oid2 = pm.getObjectId(t2);
        oid3 = pm.getObjectId(t3);
        oid4 = pm.getObjectId(t4);
        oid5 = pm.getObjectId(t5);
        
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
	public void testPathFunctionFail() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		checkSetFilterFails(pm, "_ref2");
		checkSetFilterFails(pm, "_ref2");
		checkSetFilterFails(pm, "_ref2 == 3");
		checkSetFilterFails(pm, "_ref2 = 3");
		checkSetFilterFails(pm, "_ref2 == 'null'");
		checkSetFilterFails(pm, "_ref2 > _ref1");

		checkSetFilterFails(pm, "_ref2.");
		checkSetFilterFails(pm, "_ref2. == 3");
		checkSetFilterFails(pm, "_ref2. = 3");
		checkSetFilterFails(pm, "_ref2. == 'null'");
		checkSetFilterFails(pm, "_ref2. > _ref1");

		checkSetFilterFails(pm, "_ref2._ref2");
		checkSetFilterFails(pm, "_ref2._ref2 == 3");
		checkSetFilterFails(pm, "_ref2._ref2 = 3");
		checkSetFilterFails(pm, "_ref2._ref2 == 'null'");
		checkSetFilterFails(pm, "_ref2._ref2 > _ref1");

		checkSetFilterFails(pm, "this.this._ref._int > 1");
		checkSetFilterFails(pm, "this.this._int > 1");
		
		
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
	public void testRefSingleString() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("_ref2._string.matches('xyz1')");
		checkString(q, "xyz1");

		q.setFilter("this._ref2._string.matches('xyz')");
		checkString(q);

		q.setFilter("this._ref2._string.matches('.*3.*')");
		checkString(q, "xyz3");

		q.setFilter("_ref2._string.matches('.*y.*')");
		checkString(q, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		q.setFilter("_ref2._string.startsWith('xyz1')");
		checkString(q, "xyz1");

		q.setFilter("_ref2._string.startsWith('xyz')");
		checkString(q, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		q.setFilter("_ref2._string.startsWith('xyz12')");
		checkString(q);

		q.setFilter("_ref2._string.endsWith('xyz1')");
		checkString(q, "xyz1");

		q.setFilter("_ref2._string.endsWith('yz1')");
		checkString(q, "xyz1");

		q.setFilter("_ref2._string.endsWith('xyz12')");
		checkString(q);

		//non-JDO:
		q.setFilter("_ref2._string.contains('xyz1')");
		checkString(q, "xyz1");

		q.setFilter("_ref2._string.contains('yz1')");
		checkString(q, "xyz1");

		q.setFilter("_ref2._string.contains('xyz12')");
		checkString(q);

		q.setFilter("_ref2._string.contains('xyz')");
		checkString(q, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		TestTools.closePM();
	}
	
	@Test
	public void testRefDoubleString() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("_ref2._ref2._string.matches('xyz1')");
		checkString(q, "xyz1");

		q.setFilter("_ref2._ref2._string.matches('xyz')");
		checkString(q);

		q.setFilter("_ref2._ref2._string.matches('.*3.*')");
		checkString(q, "xyz3");

		q.setFilter("_ref2._ref2._string.matches('.*y.*')");
		checkString(q, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		q.setFilter("_ref2._ref2._string.startsWith('xyz1')");
		checkString(q, "xyz1");

		q.setFilter("_ref2._ref2._string.startsWith('xyz')");
		checkString(q, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		q.setFilter("_ref2._ref2._string.startsWith('xyz12')");
		checkString(q);

		q.setFilter("_ref2._ref2._string.endsWith('xyz1')");
		checkString(q, "xyz1");

		q.setFilter("_ref2._ref2._string.endsWith('yz1')");
		checkString(q, "xyz1");

		q.setFilter("_ref2._ref2._string.endsWith('xyz12')");
		checkString(q);

		//non-JDO:
		q.setFilter("_ref2._ref2._string.contains('xyz1')");
		checkString(q, "xyz1");

		q.setFilter("_ref2._ref2._string.contains('yz1')");
		checkString(q, "xyz1");

		q.setFilter("_ref2._ref2._string.contains('xyz12')");
		checkString(q);

		q.setFilter("_ref2._ref2._string.contains('xyz')");
		checkString(q, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		TestTools.closePM();
	}
	
	@Test
	public void testLoops() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("_ref2._int == 12345");
		checkOid(q, oid5);

		q.setFilter("_ref2._ref2._int == 12345");
		checkOid(q, oid5);

		q.setFilter("_ref2._ref2._ref2._int == 12345");
		checkOid(q, oid5);

		q.setFilter("_ref2._int == 1");
		checkOid(q, oid3);

		q.setFilter("_ref2._ref2._int == 1");
		checkOid(q, oid2);

		q.setFilter("_ref2._ref2._ref2._int == 1");
		checkOid(q, oid1);

		q.setFilter("_ref2._ref2._ref2._ref2._int == 1");
		checkOid(q, oid3);

		q.setFilter("_ref2._ref2._ref2._ref2._ref2._ref2._int == 1");
		checkOid(q, oid2);

		q.setFilter("_ref2._ref2._ref2._ref2._ref2._ref2._ref2._int == 1");
		checkOid(q, oid1);

		q.setFilter("_ref2._ref2._ref2._ref2._ref2._ref2._ref2._int < 1");
		checkOid(q);

		TestTools.closePM();
	}
	
	@Test
	public void testNull() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestClass.class);

		q.setFilter("_ref2._ref2._ref2._ref2._ref2._ref2._ref2 != null");
		checkOidWithParam(null, q, oid1, oid2, oid3, oid4);

		q.setFilter("_ref2._ref2._ref2._ref2._ref2._ref2._ref2 == null");
		checkOidWithParam(null, q);

		q.setFilter("_ref2 != null");
		checkOidWithParam(null, q, oid1, oid2, oid3, oid4);

		q.setFilter("_ref2 == null");
		checkOidWithParam(null, q, oid5);

		q.setFilter("_ref2 != :oid");
		checkOidWithParam(null, q, oid1, oid2, oid3, oid4);

		q.setFilter("_ref2 == :oid");
		checkOidWithParam(null, q, oid5);

		TestTools.closePM();
	}
	
	@Test
	public void testRefComparison() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		q = pm.newQuery(TestClass.class);

		q.setFilter("_ref2._ref2._ref2._ref2._ref2._ref2._ref2 != :oid");
		checkOidWithParam(oid1, q, oid1, oid3, oid4, oid5);

		q.setFilter("_ref2._ref2._ref2._ref2._ref2._ref2._ref2 == :oid");
		checkOidWithParam(oid1, q, oid2);

		q.setFilter("_ref2 != :oid");
		checkOidWithParam(oid1, q, oid1, oid3, oid4, oid5);

		q.setFilter("_ref2 == :oid");
		checkOidWithParam(oid1, q, oid2);

		TestTools.closePM();
	}
	
	@Test
	public void testRefSingleStringWithIndex() {
		TestTools.defineIndex(TestClass.class, "_ref2", true);
		TestTools.defineIndex(TestClass.class, "_string", true);
		testRefSingleString();
	}
	
	@Test
	public void testRefDoubleStringeWithIndex() {
		TestTools.defineIndex(TestClass.class, "_ref2", true);
		TestTools.defineIndex(TestClass.class, "_string", true);
		testRefDoubleString();
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
	
    @SuppressWarnings("unchecked")
	private void checkOid(Query q, Object ... matches) {
    	Collection<TestClass> c = (Collection<TestClass>) q.execute(); 
		for (int i = 0; i < matches.length; i++) {
			boolean match = false;
			for (TestClass t: c) {
				if (JDOHelper.getObjectId(t).equals(matches[i])) {
					match = true;
					break;
				}
			}
			assertTrue(match);
		}
		assertEquals(matches.length, c.size());
	}
	
    @SuppressWarnings("unchecked")
	private void checkOidWithParam(Object param1, Query q, Object ... matches) {
    	Object o1 = q.getPersistenceManager().getObjectById(param1);
    	Collection<TestClass> c = (Collection<TestClass>) q.execute(o1); 
		for (int i = 0; i < matches.length; i++) {
			boolean match = false;
			for (TestClass t: c) {
				if (JDOHelper.getObjectId(t).equals(matches[i])) {
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
		q.setFilter("_ref2.listObj.isEmpty()");
		checkString(q, "0000");

		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("_ref2.listObj.contains(1234)");
		checkString(q, "1111");

		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("_ref2.listObj.contains(1234L)");
		checkString(q);

		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("_ref2.listObj.contains(123)");
		checkString(q);
   }
	
    @Test
    public void testListTC() {
    	Object oid1 = populateTQC()[1];
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("_ref2.listTC.isEmpty()");
		checkString(q, "0000");

		Object o1 = pm.getObjectById(oid1);
		q = pm.newQuery(TestQueryClass.class);
		q.setFilter("_ref2.listTC.contains(:o1)");
		checkString(q, o1, "1111");

		//use OID as parameter, TODO doesn't work yet
		//q = pm.newQuery(TestQueryClass.class);
		//q.setFilter("listTC.contains(:oid1)");
		//checkString(q, oid1, "1111");
   }
	
    @Test
    public void testMap() {
    	populateTQC();
  		PersistenceManager pm = TestTools.openPM();
  		pm.currentTransaction().begin();

  		Query q = null; 

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("_ref2.coll.isEmpty()");
  		checkString(q, "0000");

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("_ref2.coll.contains('coll')");
  		checkString(q, "1111");

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("_ref2.coll.contains(null)");
  		checkString(q);

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("_ref2.listObj.contains('123')");
  		checkString(q);
  		
  		TestTools.closePM();
    }
	
    @Test
    public void testCollections() {
    	populateTQC();
  		PersistenceManager pm = TestTools.openPM();
  		pm.currentTransaction().begin();

  		Query q = null; 

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("_ref2.map.isEmpty()");
  		checkString(q, "0000");

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("_ref2.map.containsKey('key')");
  		checkString(q, "1111");

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("_ref2.map.containsKey(null)");
  		checkString(q);

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("_ref2.map.containsKey('123')");
  		checkString(q);
  		
  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("_ref2.map.containsValue(_ref2)");
  		checkString(q, "1111");

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("_ref2.map.containsValue(null)");
  		checkString(q);

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("_ref2.map.containsValue('123')");
  		checkString(q);
  		
  		TestTools.closePM();
    }
    
    @Test
    public void testThis() {
   		PersistenceManager pm = TestTools.openPM();
  		pm.currentTransaction().begin();

  		Query q = null; 

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("this == this");
  		checkOid(q, oid1, oid2, oid3, oid4, oid5);

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("this != this");
  		checkOid(q);

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("this == _ref2");
  		checkOid(q, oid5);

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("this == this._ref2");
  		checkOid(q, oid5);
  		
  		TestTools.closePM();
    }
    
    @Test
    public void testRhsRefs() {
    	Object[] oids = populateTQC();
  		PersistenceManager pm = TestTools.openPM();
  		pm.currentTransaction().begin();

  		Query q = null; 

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("listTC.contains(_ref2)");
  		checkOid(q, oids[1]);

  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("map.containsKey(_ref2)");
  		checkOid(q, oids[1]);
  		
  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("_ref2._ref2 == this");
  		checkOid(q, oids[1], oids[2]);
  		
  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("_ref2.listTC.contains(this)");
  		checkOid(q, oids[1]);
  		
  		q = pm.newQuery(TestQueryClass.class);
  		q.setFilter("_ref2.listTC.contains(_ref2._ref2)");
  		checkOid(q, oids[1]);
  		
  		TestTools.closePM();
    }
    
    private Object[] populateTQC() {
  		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		//nulls
		TestQueryClass tN = new TestQueryClass(); 
		tN.setString("NULL");
		pm.makePersistent(tN);
		Object oid0 = JDOHelper.getObjectId(tN);

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
		t2.addObj(new Integer(1234));
		t2.addTC(t1);
		t2.addToMap("key", t1);
		t2.addToSet("123");
		t2.addToColl("coll");
		t2.setRef2(t1);
		pm.makePersistent(t2);
		Object oid2 = JDOHelper.getObjectId(t2);
		
		t1.setRef2(t2);
		t2.setRef2(t1);
		
		pm.currentTransaction().commit();
    	TestTools.closePM();
    	return new Object[]{oid0, oid1, oid2};
    }
	
}

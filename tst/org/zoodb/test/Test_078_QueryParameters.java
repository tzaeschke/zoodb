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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.fail;

import java.util.Collection;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.util.TestTools;

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
public class Test_078_QueryParameters {

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
        tc1.setData(1, false, 'c', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
        		-1.1f, 35);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(12, false, 'd', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
        		-0.1f, 34);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(123, false, 'e', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
        		0.1f, 3.0);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(1234, false, 'f', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
        		1.1f, -0.01);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(12345, false, 'g', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
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
	public void testParameters() {
		internalTestParameters(TYPE.CLASS_QUERY);
		internalTestParameters(TYPE.WHERE_QUERY);
		internalTestParameters(TYPE.SET_FILTER);
	}
	
	
	/**
	 * Queries used to fail if the string ended with true/false.
	 */
	private void internalTestParameters(TYPE type) {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		Collection<TestClass> c = null;
		
		int i12 = 12;
		q = newQuery(pm, "_int == intParam parameters int intParam", type);
		//pm.newQuery(TestClass.class, "_int == intParam parameters int intParam");
		c = (Collection<TestClass>)q.execute(i12);
		assertEquals(1, c.size());

		//test left-hand
		q = newQuery(pm, "intParam == _int parameters int intParam", type);
		c = (Collection<TestClass>)q.execute(i12);
		assertEquals(1, c.size());

		String str = "xyz";
		q = pm.newQuery(TestClass.class, "_string == strParam parameters String strParam");
		c = (Collection<TestClass>)q.execute(str);
		assertEquals(5, c.size());

		q = pm.newQuery(TestClass.class, "_string == strParam parameters String strParam");
		c = (Collection<TestClass>)q.execute(null);
		assertEquals(0, c.size());
		//TODO check result with one actually having 'null'

		q = pm.newQuery(TestClass.class, "_string == strParam && _int > intParam parameters String strParam int intparam");
		c = (Collection<TestClass>)q.execute(str, i12);
		assertEquals(1, c.size());
		TestClass t = c.toArray(new TestClass[1])[0];
		assertEquals(i12, t.getInt());
		assertEquals(str, t.getString());
		
		
		TestTools.closePM();
	}
	
	@Test
	public void testParameterErrors() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null;
		Collection<TestClass> c = null;
		
		int i12 = 12;
		q = pm.newQuery(TestClass.class, "_int == intParam parameters int intParam");
		c = (Collection<TestClass>)q.execute("123"); //TODO shold fail
		//TODO check result
		
		String str = "xyz";
		q = pm.newQuery(TestClass.class, "_string == strParam parameters String strParam");
		c = (Collection<TestClass>)q.execute(123);
		//TODO check result

		//too many params
		q = pm.newQuery(TestClass.class, "_string == strParam parameters String strParam");
		c = (Collection<TestClass>)q.execute(str, i12);
		//TODO check result

		q = pm.newQuery(TestClass.class, "_string == strParam parameters String strParam");
		c = (Collection<TestClass>)q.execute(null, null);
		//TODO check result

		//missing param
		q = pm.newQuery(TestClass.class, "_string == strParam && _int > intParam parameters String strParam int intparam");
		c = (Collection<TestClass>)q.execute(str);
		//TODO check result

		//missing param
		q = pm.newQuery(TestClass.class, "_string == strParam parameters String strParam");
		c = (Collection<TestClass>)q.execute();
		//TODO check result

		//wrong order
		q = pm.newQuery(TestClass.class, "_string == strParam && _int > intParam parameters String strParam int intparam");
		c = (Collection<TestClass>)q.execute(123, "xxx");
		//TODO check result

		//too many declared
		q = pm.newQuery(TestClass.class, "_string == strParam parameters String strParam int intparam");
		c = (Collection<TestClass>)q.execute();
		//TODO check result

		//missing declaration
		q = pm.newQuery(TestClass.class, "_string == strParam parameters String strParam int intparam");
		c = (Collection<TestClass>)q.execute("xxx", 123);
		//TODO check result

		//too many declared
		q = pm.newQuery(TestClass.class, "parameters String strParam");
		c = (Collection<TestClass>)q.execute();
		//TODO check result

		q = newQuery(pm, "parameters String strParam", TYPE.CLASS_QUERY);
		checkFail(q);
		q = newQuery(pm, "parameters String strParam", TYPE.SET_FILTER);
		checkFail(q);
		q = newQuery(pm, "parameters String strParam", TYPE.WHERE_QUERY);
		checkFail(q);
		
		
		TestTools.closePM();
	}
	

	@Test
	public void testImplicitParameters() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		Collection<TestClass> c = null;
		
		int i12 = 12;
		pm.newQuery(TestClass.class, "_int == :intParam");
		c = (Collection<TestClass>)q.execute(i12);
		assertEquals(1, c.size());

		//test left-hand
		pm.newQuery(TestClass.class, ":intParam == _int");
		c = (Collection<TestClass>)q.execute(i12);
		assertEquals(1, c.size());

		
		String str = "xyz";
		q = pm.newQuery(TestClass.class, "_string == strParam parameters String strParam");
		c = (Collection<TestClass>)q.execute(str);
		assertEquals(5, c.size());

		q = pm.newQuery(TestClass.class, "_string == strParam parameters String strParam");
		c = (Collection<TestClass>)q.execute(null);
		assertEquals(0, c.size());
		//TODO check result with one actually having 'null'

		q = pm.newQuery(TestClass.class, "_string == strParam && _int > intParam parameters String strParam int intparam");
		c = (Collection<TestClass>)q.execute(str, i12);
		assertEquals(1, c.size());
		TestClass t = c.toArray(new TestClass[1])[0];
		assertEquals(i12, t.getInt());
		assertEquals(str, t.getString());
		
		TestTools.closePM();
	}
	
	@Test
	public void testImplicitParameterErrors() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		Collection<TestClass> c = null;
		
		int i12 = 12;
		
		//implicit + explicit
		pm.newQuery(TestClass.class, "_int == :intParam PARAMETERS int intParam");
		c = (Collection<TestClass>)q.execute(i12);
		assertEquals(1, c.size());
		//TODO fail
		
		String str = "xyz";
		q = pm.newQuery(TestClass.class, "_string == :strParam parameters String strParam");
		c = (Collection<TestClass>)q.execute(str);
		//TODO fail

		q = pm.newQuery(TestClass.class, "_string == :strParam");
		q.declareParameters("String strParam");
		c = (Collection<TestClass>)q.execute(null);
		//TODO fail

		q = pm.newQuery(TestClass.class, "_string == :strParam");
		q.declareParameters("String :strParam");
		c = (Collection<TestClass>)q.execute(str, i12);
		//TODO fail
		
		TestTools.closePM();
	}
	
	
	private enum TYPE {
		SET_FILTER,
		CLASS_QUERY,
		WHERE_QUERY;
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
	
	private void checkFail(Query q, Object ...params ) {
		try {
			q.executeWithArray(params);
			fail();
		} catch (Throwable t) {
			//good
		}
	}
	
}

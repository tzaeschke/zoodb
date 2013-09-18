/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

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
	@SuppressWarnings("unchecked")
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
		//TODO
		System.err.println("TODO implement LHS queries.");
//		q = newQuery(pm, "intParam == _int parameters int intParam", type);
//		c = (Collection<TestClass>)q.execute(i12);
//		assertEquals(1, c.size());

		String str = "xyz";
		q = pm.newQuery(TestClass.class, "_string == strParam parameters String strParam");
		c = (Collection<TestClass>)q.execute(str);
		assertEquals(5, c.size());

		q = pm.newQuery(TestClass.class, "_string == strParam parameters String strParam");
		c = (Collection<TestClass>)q.execute(null);
		assertEquals(0, c.size());

		q = pm.newQuery(TestClass.class, "_string == strParam && _int == intParam " +
				"parameters String strParam int intParam");
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
		
		int i12 = 12;
		q = pm.newQuery(TestClass.class, "_int == intParam parameters int intParam");
		//should fail, wrong argument type
		checkFail(q, "123");  
		
		String str = "xyz";
		q = pm.newQuery(TestClass.class, "_string == strParam parameters String strParam");
		//should fail, wrong argument type
		checkFail(q, 123);

		//too many params
		q = pm.newQuery(TestClass.class, "_string == strParam parameters String strParam");
		//should fail, too many arguments
		checkFail(q, str, i12);

		q = pm.newQuery(TestClass.class, "_string == strParam parameters String strParam");
		//should fail, too many arguments
		checkFail(q, null, null);

		//missing param
		q = pm.newQuery(TestClass.class, "_string == strParam && _int > intParam " +
				"parameters String strParam int intParam");
		//should fail, too few arguments
		checkFail(q, str);

		//missing param
		q = pm.newQuery(TestClass.class, "_string == strParam parameters String strParam");
		checkFail(q);

		//wrong order
		q = pm.newQuery(TestClass.class, "_string == strParam && _int > intParam " +
				"parameters String strParam int intParam");
		checkFail(q, 123, "xxx");

		//too many declared
		checkFail(pm, "_string == strParam parameters String strParam int intParam");

		//missing declaration
		q = pm.newQuery(TestClass.class, "_string == strParam");
		checkFail(q, "xxx");

		//missing filter
		checkFail(pm, "parameters String strParam");

		//misspelled declaration: 'p' vs 'P'
		checkFail(pm, "_string == strParam && _int > intParam " +
				"parameters String strParam int intparam");
		
		
		checkFail(pm, "parameters String strParam", TYPE.CLASS_QUERY);
		checkFail(pm, "parameters String strParam", TYPE.SET_FILTER);
		checkFail(pm, "parameters String strParam", TYPE.WHERE_QUERY);
		
		
		TestTools.closePM();
	}
	

	@SuppressWarnings("unchecked")
	@Test
	public void testImplicitParameters() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		Collection<TestClass> c = null;
		int i12 = 12;

		q = pm.newQuery(TestClass.class, "_int == :intParam");
		c = (Collection<TestClass>)q.execute(i12);
		assertEquals(1, c.size());

		//test left-hand
		//TODO
		System.err.println("TODO implement LHS queries.");
//		q = pm.newQuery(TestClass.class, ":intParam == _int");
//		c = (Collection<TestClass>)q.execute(i12);
//		assertEquals(1, c.size());
		
		String str = "xyz";
		q = pm.newQuery(TestClass.class, "_string == strParam parameters String strParam");
		c = (Collection<TestClass>)q.execute(str);
		assertEquals(5, c.size());

		q = pm.newQuery(TestClass.class, "_string == strParam parameters String strParam");
		c = (Collection<TestClass>)q.execute(null);
		assertEquals(0, c.size());

		q = pm.newQuery(TestClass.class, "_string == strParam && _int == intParam " +
				"parameters String strParam int intParam");
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
		
		//implicit + explicit
		checkFail("Duplicate", pm, "_int == :intParam PARAMETERS int intParam");
		
		checkFail("Duplicate", pm, "_string == :strParam parameters String strParam");

		q = pm.newQuery(TestClass.class, "_string == :strParam");
		try {
			q.declareParameters("String strParam");
			fail();
		} catch (JDOUserException e) {
			assertTrue(e.getMessage().contains("Duplicate"));
		}

		q = pm.newQuery(TestClass.class, "_string == :strParam");
		try {
			q.declareParameters("String :strParam");
		} catch (JDOUserException e) {
			//illegal parameter name
		}
		
		TestTools.closePM();
	}
	
	
	private enum TYPE {
		SET_FILTER,
		CLASS_QUERY,
		WHERE_QUERY;
	}
	
	private void checkFail(PersistenceManager pm, String str, TYPE type) {
		try {
			switch (type) {
			case SET_FILTER:
				Query q = pm.newQuery(TestClass.class);
				q.setFilter(str);
				break;
			case CLASS_QUERY: 
				pm.newQuery(TestClass.class, str);
				break;
			case WHERE_QUERY:
				pm.newQuery("SELECT FROM " + TestClass.class.getName() + " WHERE " + str);
				break;
			default: throw new IllegalArgumentException();
			}
			fail();
		} catch (JDOUserException e) {
			//good
		}
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

	private void checkFail(PersistenceManager pm, String query) {
		try {
			pm.newQuery(TestClass.class, query);
			fail();
		} catch (Throwable t) {
			//good
		}
	}

	private void checkFail(String msgPart, PersistenceManager pm, String query) {
		try {
			pm.newQuery(TestClass.class, query);
			fail();
		} catch (Throwable t) {
			//good
			assertTrue(t.getMessage(), t.getMessage().contains(msgPart));
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testMultiParameters() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		Collection<TestClass> c = null;
		
		//implicit + explicit
		q = pm.newQuery(TestClass.class, "_int == intParam || _short == shortParam " +
				"PARAMETERS int intParam short shortParam");
		c = (Collection<TestClass>)q.execute(12, (short)32003);
		assertEquals(2, c.size());
		
		q = pm.newQuery(TestClass.class, "_int == :intParam || _short == :shortParam || " +
				"_byte == :byteParam");
		c = (Collection<TestClass>)q.execute(12, (short)32003, (byte)123);
		assertEquals(3, c.size());

		q = pm.newQuery(TestClass.class, "_int == :intParam || _short == :shortParam || " +
				"_byte == :byteParam");
		c = (Collection<TestClass>)q.execute(12, (short)32003, (byte)123);
		assertEquals(3, c.size());

		q = pm.newQuery(TestClass.class, "_int == :intParam || _short == :shortParam || " +
				"_byte == :byteParam");
		c = (Collection<TestClass>)q.executeWithArray(12, (short)32003, (byte)123);
		assertEquals(3, c.size());

		q = pm.newQuery(TestClass.class, "_int == :intParam || _short == :shortParam || " +
				"_byte == :byteParam");
		Object[] params = new Object[]{12, (short)32003, (byte)123};
		c = (Collection<TestClass>)q.executeWithArray(params);
		assertEquals(3, c.size());

		q = pm.newQuery(TestClass.class, "_int == :intParam || _short == :shortParam || " +
				"_byte == :byteParam");
		Map<String, Object> paramMap = new HashMap<String, Object>();
		paramMap.put("intParam", 12);
		paramMap.put("shortParam", (short)32003);
		paramMap.put("byteParam", (byte)123);
		c = (Collection<TestClass>)q.executeWithMap(paramMap);
		assertEquals(3, c.size());

		TestTools.closePM();
	}
	

	/**
	 * This test for reuse of parameters. JDO doesn't seems to specify whether this is allowed
	 * or not. But allowing this would make internal type checking harder.
	 */
	@Test
	public void testParameterReuse() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		//implicit + explicit
		checkFail(pm, "_int <= intParam || _int >= intParam PARAMETERS int intParam");
		
		checkFail(pm, "_int <= :intParam || _int >= :intParam");
		
		TestTools.closePM();
	}
}

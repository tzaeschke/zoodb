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
import java.util.HashMap;
import java.util.Map;

import javax.jdo.JDOException;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.impl.QueryImpl;
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
public class Test_174_QueryVariables {

	private static boolean q4;
	
	@BeforeClass
	public static void setUp() {
        TestTools.removeDb();
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
		q4 = QueryImpl.ENFORCE_QUERY_V4;
		QueryImpl.ENFORCE_QUERY_V4 = true;
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
		QueryImpl.ENFORCE_QUERY_V4 = q4;
		TestTools.removeDb();
	}

    @Test
    public void testVariablesVsParameters() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null;
		
		int i12 = 12;
		q = pm.newQuery(TestClass.class, "_int == intVar && intVar == intParam parameters int intParam variables int intVar");
		//should fail, wrong argument type
		checkFail(q, 123);  
	
		//TODO test using same names for VAR/PARAM
		fail();
		
		String str = "xyz";
		q = pm.newQuery(TestClass.class, "_string == strParam variables String strParam");
		//should fail, wrong argument type
		checkFail(q, 123);

		q = pm.newQuery(TestClass.class, "_string == strParam variables int strParam");
		//should fail, wrong parameter type
		checkFail(q, str);

		q = pm.newQuery(TestClass.class, "_string == strParam variables Integer strParam");
		//should fail, wrong parameter type
		checkFail(q, str);

		//too many params
		q = pm.newQuery(TestClass.class, "_string == strParam variables String strParam");
		//should fail, too many arguments
		checkFail(q, str, i12);

		q = pm.newQuery(TestClass.class, "_string == strParam variables String strParam");
		//should fail, too many arguments
		checkFail(q, null, null);

		//missing param
		q = pm.newQuery(TestClass.class, "_string == strParam && _int > intParam " +
				"variables String strParam, int intParam");
		//should fail, too few arguments
		checkFail(q, str);

		//missing param
		q = pm.newQuery(TestClass.class, "_string == strParam variables String strParam");
		checkFail(q);

		//wrong order
		q = pm.newQuery(TestClass.class, "_string == strParam && _int > intParam " +
				"variables String strParam, int intParam");
		checkFail(q, 123, "xxx");

		//too many declared
		checkFail(pm, "_string == strParam variables String, strParam int intParam");

		//missing declaration
		q = pm.newQuery(TestClass.class, "_string == strParam");
		checkFail(q, "xxx");

		//missing filter
		checkFail(pm, "variables String strParam");

		//misspelled declaration: 'p' vs 'P'
		checkFail(pm, "_string == strParam && _int > intParam " +
				"variables String strParam, int intparam");
		
		//comma missing
		checkFail(pm, "_string == strParam && _int > intParam " +
				"variables String strParam int intparam");
		
		
		checkFail(pm, "variables String strParam", TYPE.CLASS_QUERY);
		checkFail(pm, "variables String strParam", TYPE.SET_FILTER);
		checkFail(pm, "variables String strParam", TYPE.WHERE_QUERY);
		
		
		TestTools.closePM();
    }
	
    @Test
    public void testVariablesUnconstrained() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null;
		
		q = pm.newQuery(TestClass.class, "_int == intVar variables int intVar");
		//should fail, unconstrained var
		checkFailUC(q);  
		q = pm.newQuery(TestClass.class, "_int == intVar");
		q.declareVariables("int intVar");
		//should fail, unconstrained var
		checkFailUC(q);  


		q = pm.newQuery(TestClass.class, "_int == intVar && intVar >= 1 variables int intVar");
		//should fail, unconstrained var
		checkFailUC(q);  
		q = pm.newQuery(TestClass.class, "_int == intVar && intVar >= 1");
		q.declareVariables("int intVar");
		//should fail, unconstrained var
		checkFailUC(q);  


		q = pm.newQuery(TestClass.class, "intVar >= 1 && _int == intVar variables int intVar");
		//should fail, unconstrained var
		checkFailUC(q);  
		q = pm.newQuery(TestClass.class, "intVar >= 1 && _int == intVar");
		q.declareVariables("int intVar");
		//should fail, unconstrained var
		checkFailUC(q);  


		q = pm.newQuery(TestClass.class, "(intVar == 1 || intVar >= 5) && _int == intVar variables int intVar");
		//should fail, unconstrained var
		checkFailUC(q);  
		q = pm.newQuery(TestClass.class, "(intVar == 1 || intVar >= 5) && _int == intVar");
		q.declareVariables("int intVar");
		//should fail, unconstrained var
		checkFailUC(q);  


		checkFail(pm, "variables String strParam", TYPE.CLASS_QUERY);
		checkFail(pm, "variables String strParam", TYPE.SET_FILTER);
		checkFail(pm, "variables String strParam", TYPE.WHERE_QUERY);
		
		
		TestTools.closePM();
    }
	
	@Test
	public void testVariablesPartialConstraint() {
		internalTestVariablesPartialConstraint(TYPE.CLASS_QUERY);
		internalTestVariablesPartialConstraint(TYPE.WHERE_QUERY);
		internalTestVariablesPartialConstraint(TYPE.SET_FILTER);
	}
	
	
	/**
	 * Queries used to fail if the string ended with true/false.
	 */
	@SuppressWarnings("unchecked")
	private void internalTestVariablesPartialConstraint(TYPE type) {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		Collection<TestClass> c = null;
		
		//Constrain variable partially (only in first term), don't use it in 2nd term 
		q = newQuery(pm, "(_int == intVar && intVar == 12) || (_int == 333)", type);
		q.declareVariables("int intVar");
		//pm.newQuery(TestClass.class, "_int == intParam parameters int intParam");
		c = (Collection<TestClass>)q.execute();
		assertEquals(1, c.size());
		
		TestTools.closePM();
	}
	
	@Test
	public void testVariables() {
		internalTestVariables(TYPE.CLASS_QUERY);
		internalTestVariables(TYPE.WHERE_QUERY);
		internalTestVariables(TYPE.SET_FILTER);
	}
	
	
	/**
	 * Queries used to fail if the string ended with true/false.
	 */
	@SuppressWarnings("unchecked")
	private void internalTestVariables(TYPE type) {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		Collection<TestClass> c = null;
		
		int i12 = 12;
		q = newQuery(pm, "_int == intVar && intVar == 12 variables int intVar", type);
		//pm.newQuery(TestClass.class, "_int == intParam parameters int intParam");
		c = (Collection<TestClass>)q.execute();
		assertEquals(1, c.size());

		//test left-hand
		q = newQuery(pm, "intVar == _int && intVar == 12 variables int intVar", type);
		c = (Collection<TestClass>)q.execute();
		assertEquals(1, c.size());

		String str = "xyz";
		q = pm.newQuery(TestClass.class, "_string == strVar && strVar == 'xyz' variable String strVar");
		c = (Collection<TestClass>)q.execute();
		assertEquals(5, c.size());

		q = pm.newQuery(TestClass.class, "_string == strVar && strVar == 'xyz' variable String strVar");
		c = (Collection<TestClass>)q.execute(null);
		assertEquals(0, c.size());

		q = pm.newQuery(TestClass.class, "_string == strVar && _int == intVar " +
				" && strVar == 'xyz' && intVar == 12 variable String strVar, int intVar");
		c = (Collection<TestClass>)q.execute();
		assertEquals(1, c.size());
		TestClass t = c.iterator().next();
		assertEquals(i12, t.getInt());
		assertEquals(str, t.getString());
		
		
		TestTools.closePM();
	}
	
	@Test 
	public void testCombos() {
		//TODO test 'OR' combos of constrained terms with other terms
		//Question: 
		//- Do we determine the type of the QueryAdvice correctly? 
		//- Are the iterators/values handled correctly?
		//"_int > 2 && (_list.contains(x) || _int == x._int) && x._int == 17";
		System.err.println("Implement Test_074... testCombos()");
		fail();
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

		q = pm.newQuery(TestClass.class, "_string == strParam parameters int strParam");
		//should fail, wrong parameter type
		checkFail(q, str);

		q = pm.newQuery(TestClass.class, "_string == strParam parameters Integer strParam");
		//should fail, wrong parameter type
		checkFail(q, str);

		//too many params
		q = pm.newQuery(TestClass.class, "_string == strParam parameters String strParam");
		//should fail, too many arguments
		checkFail(q, str, i12);

		q = pm.newQuery(TestClass.class, "_string == strParam parameters String strParam");
		//should fail, too many arguments
		checkFail(q, null, null);

		//missing param
		q = pm.newQuery(TestClass.class, "_string == strParam && _int > intParam " +
				"parameters String strParam, int intParam");
		//should fail, too few arguments
		checkFail(q, str);

		//missing param
		q = pm.newQuery(TestClass.class, "_string == strParam parameters String strParam");
		checkFail(q);

		//wrong order
		q = pm.newQuery(TestClass.class, "_string == strParam && _int > intParam " +
				"parameters String strParam, int intParam");
		checkFail(q, 123, "xxx");

		//too many declared
		checkFail(pm, "_string == strParam parameters String strParam, int intParam");

		//wrong comma
		checkFail(pm, "_string == strParam && _int == intParam parameters String, strParam int intParam");

		//missing comma
		checkFail(pm, "_string == strParam && _int == intParam parameters String strParam int intParam");

		//missing declaration
		q = pm.newQuery(TestClass.class, "_string == strParam");
		checkFail(q, "xxx");

		//missing filter
		checkFail(pm, "parameters String strParam");

		//misspelled declaration: 'p' vs 'P'
		checkFail(pm, "_string == strParam && _int > intParam " +
				"parameters String strParam, int intparam");
		
		//comma missing
		checkFail(pm, "_string == strParam && _int > intParam " +
				"parameters String strParam int intparam");
		
		
		checkFail(pm, "parameters String strParam", TYPE.CLASS_QUERY);
		checkFail(pm, "parameters String strParam", TYPE.SET_FILTER);
		checkFail(pm, "parameters String strParam", TYPE.WHERE_QUERY);
		
		
		TestTools.closePM();
	}
	

	@Test
	public void testParameterErrorsLHS() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		try {
			Query q = pm.newQuery("SELECT FROM " + TestClass.class.getName() + " WHERE param > 0");
			Collection<?> c = (Collection<?>) q.execute();
			assertEquals(0, c.size());
		} catch (JDOUserException e) {
			//good, class not found, cannot be materialized
		}

		Query q;
		
		int i12 = 12;
		q = pm.newQuery(TestClass.class, "intParam == _int parameters int intParam");
		//should fail, wrong argument type
		checkFail(q, "123");  
		
		String str = "xyz";
		q = pm.newQuery(TestClass.class, "strParam == _string parameters String strParam");
		//should fail, wrong argument type
		checkFail(q, 123);

		//too many params
		q = pm.newQuery(TestClass.class, "strParam == _string parameters String strParam");
		//should fail, too many arguments
		checkFail(q, str, i12);

		q = pm.newQuery(TestClass.class, "strParam == _string parameters String strParam");
		//should fail, too many arguments
		checkFail(q, null, null);

		//missing param
		q = pm.newQuery(TestClass.class, "strParam == _string && intParam < _int " +
				"parameters String strParam, int intParam");
		//should fail, too few arguments
		checkFail(q, str);

		//missing param
		q = pm.newQuery(TestClass.class, "strParam == _string parameters String strParam");
		checkFail(q);

		//wrong order
		q = pm.newQuery(TestClass.class, "strParam == _string && intParam > _int " +
				"parameters String strParam, int intParam");
		checkFail(q, 123, "xxx");

		//too many declared
		checkFail(pm, "strParam == _string parameters String strParam, int intParam");

		//wrong comma
		checkFail(pm, "strParam == _string && _int == intParam parameters String, strParam int intParam");

		//missing comma
		checkFail(pm, "strParam == _string && _int == intParam parameters String strParam int intParam");

		//missing declaration
		q = pm.newQuery(TestClass.class, "strParam == _string");
		checkFail(q, "xxx");

		//missing filter
		checkFail(pm, "parameters String strParam");

		//misspelled declaration: 'p' vs 'P'
		checkFail(pm, "strParam == _string && intParam > _int " +
				"parameters String strParam, int intparam");
		
		//comma missing
		checkFail(pm, "strParam == _string && intParam < _int " +
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
		q = pm.newQuery(TestClass.class, ":intParam == _int");
		c = (Collection<TestClass>)q.execute(i12);
		assertEquals(1, c.size());
		
		String str = "xyz";
		q = pm.newQuery(TestClass.class, "_string == strParam parameters String strParam");
		c = (Collection<TestClass>)q.execute(str);
		assertEquals(5, c.size());

		q = pm.newQuery(TestClass.class, "_string == strParam parameters String strParam");
		c = (Collection<TestClass>)q.execute(null);
		assertEquals(0, c.size());

		q = pm.newQuery(TestClass.class, "_string == strParam && _int == intParam " +
				"parameters String strParam, int intParam");
		c = (Collection<TestClass>)q.execute(str, i12);
		assertEquals(1, c.size());
		TestClass t = c.iterator().next();
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
		checkFail("xplicitly declared", pm, "_int == :intParam PARAMETERS int intParam");
		
		checkFail("xplicitly declared", pm, "_string == :strParam parameters String strParam");

		q = pm.newQuery(TestClass.class, "_string == :strParam");
		try {
			q.declareParameters("String strParam");
			q.compile();
			fail();
		} catch (JDOUserException e) {
			String m = e.getMessage();
			assertTrue(m, m.contains("parameter"));
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
		WHERE_QUERY
    }
	
	private void checkFail(PersistenceManager pm, String str, TYPE type) {
		try {
			Query q;
			switch (type) {
			case SET_FILTER:
				q = pm.newQuery(TestClass.class);
				q.setFilter(str);
				break;
			case CLASS_QUERY: 
				q = pm.newQuery(TestClass.class, str);
				break;
			case WHERE_QUERY:
				q = pm.newQuery("SELECT FROM " + TestClass.class.getName() + " WHERE " + str);
				break;
			default: throw new IllegalArgumentException();
			}
			q.compile();
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
		} catch (JDOUserException t) {
			//good
		}
	}

	private void checkFailUC(Query q, Object ...params ) {
		try {
			q.executeWithArray(params);
			fail();
		} catch (JDOUserException t) {
			assertTrue(t.getMessage(), t.getMessage().contains("constrain"));
			//good
		}
	}

	private void checkFail(PersistenceManager pm, String query) {
		try {
			Query q = pm.newQuery(TestClass.class, query);
			q.compile();
			fail();
		} catch (JDOUserException t) {
			//good
		}
	}

	private void checkFail(String msgPart, PersistenceManager pm, String query) {
		try {
			Query q = pm.newQuery(TestClass.class, query);
			q.compile();
			fail();
		} catch (JDOException t) {
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
				"PARAMETERS int intParam, short shortParam");
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
	 * This test for reuse of parameters.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testParameterReuse() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		//implicit + explicit
		Query q;
		Collection<TestClass> c;
		int i12 = 12;
		
		q = pm.newQuery(TestClass.class, "_int <= intParam || _int >= intParam PARAMETERS int intParam");
		c = (Collection<TestClass>)q.execute(i12);
		assertEquals(5, c.size());
		
		q = pm.newQuery(TestClass.class, "_int <= :intParam || _int >= :intParam");
		c = (Collection<TestClass>)q.execute(i12);
		assertEquals(5, c.size());
		
		TestTools.closePM();
	}
}

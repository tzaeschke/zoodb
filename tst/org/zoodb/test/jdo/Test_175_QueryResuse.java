/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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

import java.util.Iterator;
import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.testutil.TestTools;

/**
 * Tests for query execution in parallel.
 * 
 * @author ztilmann
 *
 */
public class Test_175_QueryResuse {

	private final int nObj = 10;
	
	@BeforeClass
	public static void setUp() {
        TestTools.removeDb();
		TestTools.createDb();
	}

	@Before
	public void before() {
		TestTools.defineSchema(TestClass.class);
		PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        pm.newQuery(TestClass.class).deletePersistentAll();
        
        for (int i = 0; i < nObj; i++) {
        
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

        }
        pm.currentTransaction().commit();
        TestTools.closePM();;
	}
		
	@After
	public void afterTest() {
		TestTools.closePM();
		TestTools.removeSchema(TestClass.class);
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
	}

		
	@Test
	public void testParallelQueries() {
		internalTestParallelQueries(TYPE.CLASS_QUERY, false);
		internalTestParallelQueries(TYPE.WHERE_QUERY, false);
		internalTestParallelQueries(TYPE.SET_FILTER, false);
	}
	
	
	@Test
	public void testParallelQueriesIgnoreCach() {
		internalTestParallelQueries(TYPE.CLASS_QUERY, false);
		internalTestParallelQueries(TYPE.WHERE_QUERY, false);
		internalTestParallelQueries(TYPE.SET_FILTER, false);
	}
	
	
	@Test
	public void testParallelQueriesWithIndex() {
		TestTools.defineIndex(TestClass.class, "_int", false);
		internalTestParallelQueries(TYPE.CLASS_QUERY, true);
		internalTestParallelQueries(TYPE.WHERE_QUERY, true);
		internalTestParallelQueries(TYPE.SET_FILTER, true);
	}
	
	
	@Test
	public void testParallelQueriesWithIndexIgnoreCache() {
		TestTools.defineIndex(TestClass.class, "_int", false);
		internalTestParallelQueries(TYPE.CLASS_QUERY, true);
		internalTestParallelQueries(TYPE.WHERE_QUERY, true);
		internalTestParallelQueries(TYPE.SET_FILTER, true);
	}
	
	
	@SuppressWarnings("unchecked")
	private void internalTestParallelQueries(TYPE type, boolean ignoreCache) {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		q = newQuery(pm, "_int == param", type);
		q.declareParameters("int param");
		q.setIgnoreCache(ignoreCache);
		List<TestClass> list12 = (List<TestClass>) q.execute(12);
		List<TestClass> list123 = (List<TestClass>) q.execute(123);
		Iterator<TestClass> it12 = list12.iterator(); 
		Iterator<TestClass> it123 = list123.iterator(); 
		
		for (int i = 0; i < nObj; i++) {
			TestClass tc12 = it12.next();
			TestClass tc123 = it123.next();
			assertEquals(12, tc12.getInt());
			assertEquals(123, tc123.getInt());
		}
		
		TestTools.closePM();
	}

	
	@Test
	public void testQueryWithHiddenParameters() {
		testQueryWithHiddenParameters(false);
	}
	
	@Test
	public void testQueryWithHiddenParametersIgnoreCache() {
		testQueryWithHiddenParameters(true);
	}
	
	@SuppressWarnings("unchecked")
	private void testQueryWithHiddenParameters(boolean ignoreCache) {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		q = newQuery(pm, "_int == Math.abs(Math.abs(param))", TYPE.SET_FILTER);
		q.declareParameters("int param");
		q.setIgnoreCache(ignoreCache);
		List<TestClass> list12 = (List<TestClass>) q.execute(12);
		List<TestClass> list123 = (List<TestClass>) q.execute(123);
		Iterator<TestClass> it12 = list12.iterator(); 
		Iterator<TestClass> it123 = list123.iterator(); 
		
		for (int i = 0; i < nObj; i++) {
			TestClass tc12 = it12.next();
			TestClass tc123 = it123.next();
			assertEquals(12, tc12.getInt());
			assertEquals(123, tc123.getInt());
		}
		
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
	}

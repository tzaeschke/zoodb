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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
public class Test_079_QueryUnique {

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
	public void testSetUnique() {
		internalTestSetUnique(TYPE.CLASS_QUERY);
		internalTestSetUnique(TYPE.WHERE_QUERY);
		internalTestSetUnique(TYPE.SET_FILTER);
	}
	
	
	private void internalTestSetUnique(TYPE type) {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		Object o;
		
		q = newQuery(pm, "_int == 12", type);
		q.setUnique(true);
		o = q.execute();
		assertNotNull(o);
		assertTrue(o instanceof TestClass);

		q = newQuery(pm, "_int == 1234567", type);
		q.setUnique(true);
		o = q.execute();
		assertNull(o);
		
		q = newQuery(pm, "_int >= 1234", type);
		q.setUnique(true);
		try {
			o = q.execute();
			fail();
		} catch (JDOUserException e) {
			//too many results
		}
		
		TestTools.closePM();
	}
	
	@Test
	public void testUNIQUE() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		String prefix = "SELECT UNIQUE FROM " + TestClass.class.getName() + " WHERE ";
		
		Query q = null; 
		Object o;
		
		q = pm.newQuery(prefix + "_int == 12");
		o = q.execute();
		assertNotNull(o);
		assertTrue(o instanceof TestClass);

		q = pm.newQuery(prefix + "_int == 1234567");
		o = q.execute();
		assertNull(o);
		
		q = pm.newQuery(prefix + "_int >= 1234");
		try {
			o = q.execute();
			fail();
		} catch (JDOUserException e) {
			//too many results
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

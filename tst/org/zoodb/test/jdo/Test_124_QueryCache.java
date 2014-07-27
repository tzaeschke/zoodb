/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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
import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.testutil.TestTools;

public class Test_124_QueryCache {

	@BeforeClass
	public static void setUp() {
        TestTools.removeDb();
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
		TestTools.defineIndex(TestClass.class, "_int", true);
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
        tc1.setData(12, false, 'd', (byte)127, (short)32002, 1234567890L, "xyz", new byte[]{1,2},
        		-0.1f, 34);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(123, false, 'e', (byte)127, (short)32003, 1234567890L, "xyz", new byte[]{1,2},
        		0.1f, 3.0);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(1234, false, 'f', (byte)127, (short)32004, 1234567890L, "xyz", new byte[]{1,2},
        		1.1f, -0.01);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(12345, false, 'g', (byte)127, (short)32005, 1234567890L, "xyz", new byte[]{1,2},
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
	public void testQueryOnNonComittedNoMP() {
		testQueryOnNonComitted(false);
	}
	
	@Test
	public void testQueryOnNonComittedWithMP() {
		testQueryOnNonComitted(true);
	}
	
	private void testQueryOnNonComitted(boolean makePersistent) {
		PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        TestClass tc1 = new TestClass();
        tc1.setData(88, false, 'c', (byte)127, (short)11, 1234567890L, "xyz", new byte[]{1,2},
        		-1.1f, 35);
        TestClass tc2 = new TestClass();
        tc2.setData(89, false, 'c', (byte)127, (short)12, 1234567890L, "xyz", new byte[]{1,2},
        		-1.1f, 35);
        tc1.setRef2(tc2);
    	pm.makePersistent(tc1);
        if (makePersistent) {
        	pm.makePersistent(tc2);
        }

        Query q = pm.newQuery(TestClass.class);
        Collection<TestClass> r;
        boolean match = false;
        int n = 0;
        
        //Test without filter
        r = (Collection<TestClass>) q.execute();
        match = false;
        n = 0;
        for (TestClass o: r) {
        	match |= o == tc1;
        	n++;
        }
        assertTrue(match);
        assertEquals(7, n);
        checkRemoveFail(q);
        
        //Test with on index
        q.setFilter("_int >= 88 && _int <= 123");
        r = (Collection<TestClass>) q.execute();
        match = false;
        n = 0;
        for (TestClass o: r) {
        	match |= o == tc1;
        	n++;
        }
        assertTrue(match);
        assertEquals(3, n);
        checkRemoveFail(q);
       
        //Test with filter without index
        q.setFilter("_short >= 11 && _short <= 32001");
        r = (Collection<TestClass>) q.execute();
        match = false;
        n = 0;
        for (TestClass o: r) {
        	match |= o == tc1;
        	n++;
        }
        assertTrue(match);
        assertEquals(3, n);
        checkRemoveFail(q);
       
        TestTools.closePM(pm);
	}

	
	@Test
	public void testQueryOnNonComittedWithExtentNoMP() {
		testQueryOnNonComittedWithExtent(false);
	}

	@Test
	public void testQueryOnNonComittedWithExtentMP() {
		testQueryOnNonComittedWithExtent(true);
	}

	private void testQueryOnNonComittedWithExtent(boolean makePersistent) {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        TestClass tc1 = new TestClass();
        tc1.setData(88, false, 'c', (byte)127, (short)11, 1234567890L, "xyz", new byte[]{1,2},
        		-1.1f, 35);
        TestClass tc2 = new TestClass();
        tc2.setData(89, false, 'c', (byte)127, (short)12, 1234567890L, "xyz", new byte[]{1,2},
        		-1.1f, 35);
        tc1.setRef2(tc2);
    	pm.makePersistent(tc1);
        if (makePersistent) {
        	pm.makePersistent(tc2);
        }

        Extent<TestClass> x = pm.getExtent(TestClass.class);
        Query q = pm.newQuery(x);
        Collection<TestClass> r;
        boolean match = false;
        int n = 0;
        
        //Test without filter
        r = (Collection<TestClass>) q.execute();
        match = false;
        n = 0;
        for (TestClass o: r) {
        	match |= o == tc1;
        	n++;
        }
        assertTrue(match);
        assertEquals(7, n);
        checkRemoveFail(q);

        //Test with on index
        q.setFilter("_int >= 88 && _int <= 123");
        r = (Collection<TestClass>) q.execute();
        match = false;
        n = 0;
        for (TestClass o: r) {
        	match |= o == tc1;
        	n++;
        }
        assertTrue(match);
        assertEquals(3, n);
        checkRemoveFail(q);
       
        //Test with filter without index
        q.setFilter("_short >= 11 && _short <= 32001");
        r = (Collection<TestClass>) q.execute();
        match = false;
        n = 0;
        for (TestClass o: r) {
        	match |= o == tc1;
        	n++;
        }
        assertTrue(match);
        assertEquals(3, n);
        checkRemoveFail(q);
       
        TestTools.closePM(pm);
	}

	private void checkRemoveFail(Query q) {
		Collection<?> c = (Collection<?>) q.execute();
		Iterator<?> i = c.iterator();
		while (i.hasNext()) {
			i.next();
			try {
				i.remove();
				fail();
			} catch (UnsupportedOperationException e) {
				//good
			}
		}
	}
	
	@Test
	public void testParsingErrors() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        Query q = pm.newQuery("SELECT FROM " + TestClass.class.getName());

        //bad logical operator (i.e. OR instead of ||)
        checkFilterFail(q, "_int <= 123 OR _int >= 12345");
        
        //bad field name
        checkFilterFail(q, "_int <= 123 || %% >= 12345");
        
        //bad value
        checkFilterFail(q, "_int <= null || _int >= 12345");

        //bad value #2
        checkFilterFail(q, "_int <= 'hallo' || _int >= 12345");
    	
        //bad value #3
        checkFilterFail(q, "_bool == falsch || _int >= 12345");
    	
        //bad value #3
        checkFilterFail(q, "_string <= 123 || _int >= 12345");

        //bad trailing slashes
        checkFilterFail(q, "_string == '\\'");

        //bad trailing slashes
        checkFilterFail(q, "_string == 'C:\\\\Windows\\'");

        TestTools.closePM(pm);
	}
	
	private void checkFilterFail(Query q, String filter) {
		try {
			q.setFilter(filter);
			fail();
		} catch (JDOUserException e) {
			//good
		}
	}
	
}

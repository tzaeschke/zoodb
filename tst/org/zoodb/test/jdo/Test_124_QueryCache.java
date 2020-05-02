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
import java.util.Iterator;

import javax.jdo.Extent;
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
        
        //Test without filter
        checkQuery(q, 7, tc1, tc2);
        checkRemoveFail(q, 7);
        
        //Test with on index
        q.setFilter("_int >= 88 && _int <= 123");
        checkQuery(q, 3, tc1, tc2);
        checkRemoveFail(q, 3);
       
        //Test with filter without index
        q.setFilter("_short >= 11 && _short <= 32001");
        checkQuery(q, 3, tc1, tc2);
        checkRemoveFail(q, 3);
       
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
        
        //Test without filter
        checkQuery(q, 7, tc1, tc2);
        checkRemoveFail(q, 7);

        //Test with on index
        q.setFilter("_int >= 88 && _int <= 123");
        checkQuery(q, 3, tc1, tc2);
        checkRemoveFail(q, 3);
       
        //Test with filter without index
        q.setFilter("_short >= 11 && _short <= 32001");
        checkQuery(q, 3, tc1, tc2);
        checkRemoveFail(q, 3);
       
        TestTools.closePM(pm);
	}

	@SuppressWarnings("unchecked")
	private void checkQuery(Query q, int nExpected, TestClass tc1, TestClass tc2) {
		Collection<TestClass> r = (Collection<TestClass>) q.execute();
        boolean match1 = false;
        boolean match2 = false;
        int n = 0;
        for (TestClass o: r) {
        	match1 |= o == tc1;
        	match2 |= o == tc2;
        	n++;
        }
        assertTrue(match1);
        assertTrue(match2);
        assertEquals(nExpected, n);
	}

	private void checkRemoveFail(Query q, int n) {
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
}

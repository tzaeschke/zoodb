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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.testutil.TestTools;

/**
 * Tests for query setResult().
 * 
 * @author ztilmann
 *
 */
public class Test_123_QueryExtentAdaptor {

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
        
        TestClass tc1 = new TestClass();
        tc1.setData(1, false, 'c', (byte)127, (short)32001, 1234567890L, "xyz1", new byte[]{1,2},
        		-1.1f, 35);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(12, false, 'd', (byte)126, (short)32002, 1234567890L, "xyz2", new byte[]{1,2},
        		-0.1f, 34);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(123, false, 'e', (byte)125, (short)32003, 1234567890L, null, new byte[]{1,2},
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
		//also removes indexes and objects
		TestTools.removeSchema(TestClass.class);
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
	}


    /**
     * ExtentAdaptor.
     */
    @Test
    public void testMaterialization() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = pm.newQuery(TestClass.class);
		Collection<?> r = (Collection<?>)q.execute();
		
		assertEquals(5, r.size());
		assertTrue(r.contains(r.iterator().next()));
		assertTrue(r.containsAll(r));
		assertEquals(5, r.toArray().length);
		assertEquals(5, r.toArray(new TestClass[0]).length);
		assertFalse(r.isEmpty());

		pm.currentTransaction().rollback();
		TestTools.closePM();
    }
	
    /**
     * ExtentAdaptor
     */
    @SuppressWarnings("unchecked")
	@Test
    public void testImmutability() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = pm.newQuery(TestClass.class);
		Collection<TestClass> r = (Collection<TestClass>)q.execute();
		TestClass tc = new TestClass();
		
		try {
			r.add(tc);
			fail();
		} catch (UnsupportedOperationException e) {
			//good
		}

		try {
			r.remove(tc);
			fail();
		} catch (UnsupportedOperationException e) {
			//good
		}

		try {
			r.addAll(new ArrayList<TestClass>());
			fail();
		} catch (UnsupportedOperationException e) {
			//good
		}

		try {
			r.removeAll(new ArrayList<TestClass>());
			fail();
		} catch (UnsupportedOperationException e) {
			//good
		}

		try {
			r.retainAll(new ArrayList<TestClass>());
			fail();
		} catch (UnsupportedOperationException e) {
			//good
		}

		try {
			r.clear();
			fail();
		} catch (UnsupportedOperationException e) {
			//good
		}

		pm.currentTransaction().rollback();
		TestTools.closePM();
    }

    
}

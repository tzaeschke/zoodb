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

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.jdo.Extent;
import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.testutil.TestTools;

public class Test_060_Extents {

	@BeforeClass
	public static void setUp() {
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class, TestClassTiny.class, TestClassTiny2.class);
	}

	@Before
	public void beforeTest() {
	    PersistenceManager pm = TestTools.openPM();
	    pm.currentTransaction().begin();
        pm.newQuery(pm.getExtent(TestClass.class)).deletePersistentAll();
        pm.newQuery(pm.getExtent(TestClassTiny.class)).deletePersistentAll();
        pm.newQuery(pm.getExtent(TestClassTiny2.class)).deletePersistentAll();
        pm.currentTransaction().commit();
        TestTools.closePM();
	}
	
	@After
	public void afterTest() {
		TestTools.closePM();
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.closePM();
		TestTools.removeDb();
	}
	
	
    @Test
    public void testExtentOnWrongClass() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        try {
            pm.getExtent(String.class);
            fail();
        } catch (JDOUserException e) {
            //bound to fail...
        }
        
        try {
            //non persistent class
            pm.getExtent(TestClassTinyClone.class);
            fail();
        } catch (JDOUserException e) {
            //bound to fail...
        }
        
        TestTools.closePM(pm);
    }
    
	@Test
	public void testExtents() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Extent<?> ext = pm.getExtent(TestClass.class);
		assertEquals(pm, ext.getPersistenceManager());
		assertEquals(TestClass.class, ext.getCandidateClass());
		assertTrue(ext.hasSubclasses());
		
		//no instances
		ext = pm.getExtent(TestClass.class, false);
		assertFalse( ext.iterator().hasNext() );
		assertFalse(ext.hasSubclasses());

		//create pers capable
		TestClass tc = new TestClass();
		ext = pm.getExtent(TestClass.class);
		assertFalse( ext.iterator().hasNext() );
		assertTrue(ext.hasSubclasses());

		//make persistent
		pm.makePersistent(tc);
		pm.setIgnoreCache(false);
		ext = pm.getExtent(TestClass.class);
		assertTrue( ext.iterator().hasNext() );
		ext.closeAll();

		pm.setIgnoreCache(true);
		ext = pm.getExtent(TestClass.class);
		assertFalse( ext.iterator().hasNext() );

		try {
			ext.iterator().next();
			fail();
		} catch (NoSuchElementException e) {
			//great!
		}
		
		//commit
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		ext = pm.getExtent(TestClass.class);
		Iterator<?> iter = ext.iterator();
		assertTrue( iter.hasNext() );
		assertEquals(tc, iter.next());
		assertFalse( iter.hasNext() );

		//test closeAll()
		ext = pm.getExtent(TestClass.class);
		iter = ext.iterator();
		assertTrue( iter.hasNext() );
		ext.closeAll();
		assertFalse( iter.hasNext() );

		//test that subsequent (after closeAll) iterators are alright
		ext = pm.getExtent(TestClass.class);
		iter = ext.iterator();
		assertTrue( iter.hasNext() );
		assertEquals(tc, iter.next());
		assertFalse( iter.hasNext() );

		//test delete
		pm.deletePersistent(tc);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		ext = pm.getExtent(TestClass.class);
		iter = ext.iterator();
		assertFalse( iter.hasNext() );
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}

	
	/**
	 * Test extent in new transaction.
	 */
	@Test
	public void testExtentSession() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		//no instances
		Extent<TestClass> ext = pm.getExtent(TestClass.class);
		assertEquals(pm, ext.getPersistenceManager());
		assertFalse( ext.iterator().hasNext() );

		//create pers capable
		TestClass tc = new TestClass();
		//make persistent
		pm.makePersistent(tc);
		Object oid1 = JDOHelper.getObjectId(tc); 
		pm.currentTransaction().commit();
		TestTools.closePM(pm);
		
		//new transaction
		pm = TestTools.openPM();
		pm.currentTransaction().begin();

		ext = pm.getExtent(TestClass.class);
		Iterator<TestClass> iter = ext.iterator();
		assertEquals(pm, ext.getPersistenceManager());
		assertTrue( iter.hasNext() );
		TestClass tc2 = iter.next();
		assertNotSame( tc, tc2 );
		assertEquals( oid1, JDOHelper.getObjectId(tc2) );
		assertFalse( iter.hasNext() );
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}

	
	@Test
	public void testExcludingSubClass() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClassTiny t1 = new TestClassTiny();
		pm.makePersistent(t1);
		
		TestClassTiny2 t2 = new TestClassTiny2();
		pm.makePersistent(t2);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		//with subs
        Extent<TestClassTiny> ext = pm.getExtent(TestClassTiny.class, true);
        int n = 0;
        for (TestClassTiny t: ext) {
        	assertNotNull(t);
        	n++;
        }
        assertEquals(2, n);

        //without subs
        Extent<TestClassTiny> ext2 = pm.getExtent(TestClassTiny.class, false);
        n = 0;
        for (TestClassTiny t: ext2) {
        	assertEquals(t1, t);
        	n++;
        }
        assertEquals(1, n);

        //with sub only
        Extent<TestClassTiny2> ext3 = pm.getExtent(TestClassTiny2.class, true);
        n = 0;
        for (TestClassTiny2 t: ext3) {
        	assertEquals(t2, t);
        	n++;
        }
        assertEquals(1, n);
        
        ext3.closeAll();

        TestTools.closePM();
	}
	
}

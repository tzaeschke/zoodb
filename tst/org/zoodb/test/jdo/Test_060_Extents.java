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

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.jdo.Extent;
import javax.jdo.JDOFatalUserException;
import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoProperties;
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
		try {
			assertFalse(iter.hasNext());
			//no failure here. Depending on the configuration,
			//we either get 'false' or a JDOSuerException. Both is correct.
		} catch (JDOUserException e) {
			//good
		}

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
	
	/**
	 * In issue #91, a query would not check if the PM is still
	 * open if the query was executed on an indexed attribute.
	 */
	@Test
	public void testExtentOnClosedPM_Issue91() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        TestClassTiny t11 = new TestClassTiny(11, 11);
		pm.makePersistent(t11);
		TestClassTiny t12 = new TestClassTiny(12, 12);
		pm.makePersistent(t12);
		TestClassTiny2 t22 = new TestClassTiny2(21, 21, 21, 21);
		pm.makePersistent(t22);
		pm.currentTransaction().commit();

		TestTools.closePM(pm);
		
		//Class with subclass
		testExtentOnClosedPM_Issue91(TestClassTiny.class, true, false);
		testExtentOnClosedPM_Issue91(TestClassTiny.class, false, false);
		//Class without subclass
		testExtentOnClosedPM_Issue91(TestClassTiny2.class, true, true);
		testExtentOnClosedPM_Issue91(TestClassTiny2.class, false, true);
	}
	
	
	private <T> void testExtentOnClosedPM_Issue91(Class<T> cls, boolean subClasses, 
			boolean failOnClosedQueries) {
		ZooJdoProperties props = TestTools.getProps();
		props.setZooFailOnEmptyQueries(failOnClosedQueries);
        PersistenceManager pm = TestTools.openPM(props);
		pm.currentTransaction().begin();

        Extent<T> ex = pm.getExtent(cls, subClasses);
        Iterator<T> it = ex.iterator();

        pm.currentTransaction().commit();
        
        if (failOnClosedQueries) {
	        try {
	        	ex.iterator();
	        	fail();
	        } catch (JDOUserException e) {
	        	assertTrue(e.getMessage(), e.getMessage().contains("not active"));
	        }
        } else {
        	assertFalse(ex.iterator().hasNext());
        }

        if (failOnClosedQueries) {
	        try {
	        	it.hasNext();
	        	fail();
	        } catch (JDOUserException e) {
	        	assertTrue(e.getMessage(), e.getMessage().contains("closed"));
	        }
        } else {
        	assertFalse(it.hasNext());
        }
	

        if (failOnClosedQueries) {
        	try {
        		it.next();
        		fail();
        	} catch (JDOUserException e) {
        		assertTrue(e.getMessage(), e.getMessage().contains("closed"));
        	}
        } else {
        	try {
        		it.next();
        		fail();
        	} catch (NoSuchElementException e) {
        		//good
        	}
        	
        }

        try {
        	pm.getExtent(TestClass.class);
        	fail();
        } catch (JDOUserException e) {
        	assertTrue(e.getMessage(), e.getMessage().contains("not active"));
        }
        
       	//execution should work like this:
        pm.currentTransaction().setNontransactionalRead(true);
    	ex.iterator().hasNext();

    	pm.close();

        try {
        	ex.iterator();
        	fail();
        } catch (JDOFatalUserException e) {
        	assertTrue(e.getMessage(), e.getMessage().contains("closed"));
        }

        if (failOnClosedQueries) {
        	try {
        		it.hasNext();
        		fail();
        	} catch (JDOUserException e) {
        		assertTrue(e.getMessage(), e.getMessage().contains("closed"));
        	}
        } else {
        	assertFalse(it.hasNext());
        }
    	
        if (failOnClosedQueries) {
        	try {
        		it.next();
        		fail();
        	} catch (JDOUserException e) {
        		assertTrue(e.getMessage(), e.getMessage().contains("closed"));
        	}
        } else {
        	try {
        		it.next();
        		fail();
        	} catch (NoSuchElementException e) {
        		//good
        	}
        }

        TestTools.closePM(pm);
	}

}

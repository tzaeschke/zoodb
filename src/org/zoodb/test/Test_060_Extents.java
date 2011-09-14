package org.zoodb.test;

import static junit.framework.Assert.*;

import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.jdo.Extent;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class Test_060_Extents {

	@BeforeClass
	public static void setUp() {
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class, TestClassTiny.class, TestClassTiny2.class);
	}

	@Test
	public void testExtents() {
		System.out.println("Testing Extents");
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
	 * Test in new transaction.
	 */
	@Test
	public void testExtentSession() {
		System.out.println("Testing Extent of Session border");
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
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
	}
	
}

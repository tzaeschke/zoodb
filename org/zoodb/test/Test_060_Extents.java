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

	private static final String DB_NAME = "TestDb";
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb(DB_NAME);
		TestTools.defineSchema(DB_NAME, TestClass.class);
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

	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb(DB_NAME);
	}
	
}

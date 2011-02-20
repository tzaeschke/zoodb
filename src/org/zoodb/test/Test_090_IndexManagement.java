package org.zoodb.test;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.Schema;

public class Test_090_IndexManagement {

	private static final String DB_NAME = "TestDb";
	
	@Before
	public void setUp() {
        TestTools.removeDb(DB_NAME);
		TestTools.createDb(DB_NAME);
		TestTools.defineSchema(DB_NAME, TestClass.class);
	}

	@Test
	public void testNonExistentField() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Schema s = Schema.locate(pm, TestClass.class, DB_NAME);
		try {
			//non-existent field
			s.removeIndex("xy!!z");
			fail("Should have failed");
		} catch (JDOUserException e) {
			//good
		}
		
		try {
			//non-existent field
			s.isIndexDefined("xy!!z");
			fail("Should have failed");
		} catch (JDOUserException e) {
			//good
		}
		
		try {
			//non-existent field
			s.isIndexUnique("xy!!z");
			fail("Should have failed");
		} catch (JDOUserException e) {
			//good
		}
		
		try {
			//non-existent field
			s.defineIndex("xy!!z", true);
			fail("Should have failed");
		} catch (JDOUserException e) {
			//good
		}
		
		try {
			//re-create existing index
			s.defineIndex("xy!!z", false);
			fail("Should have failed");
		} catch (JDOUserException e) {
			//good
		}
		
		try {
			//non-existent field
			s.removeIndex("xy!!z");
			fail("Should have failed");
		} catch (JDOUserException e) {
			//good
		}
		TestTools.closePM(pm);
	}

	
	@Test
	public void testNonExistentIndex() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Schema s = Schema.locate(pm, TestClass.class, DB_NAME);

		//non-existent index
		assertFalse(s.removeIndex("_long"));
		
		try {
			//non-existent index
			s.isIndexUnique("_long");
			fail("Should have failed");
		} catch (JDOUserException e) {
			//good
		}

		assertFalse(s.isIndexDefined("_long"));
		
		TestTools.closePM(pm);
	}

	
	@Test
	public void testIndexCreation() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Schema s = Schema.locate(pm, TestClass.class, DB_NAME);

		s.defineIndex("_long", true);
		
		try {
			//re-create existing index
			s.defineIndex("_long", true);
			fail("Should have failed");
		} catch (JDOUserException e) {
			//good
		}
		
		try {
			//re-create existing index
			s.defineIndex("_long", false);
			fail("Should have failed");
		} catch (JDOUserException e) {
			//good
		}
		
		assertTrue(s.isIndexDefined("_long"));
		assertTrue(s.isIndexUnique("_long"));
		
		//remove
		assertTrue(s.removeIndex("_long"));
		assertFalse(s.isIndexDefined("_long"));
		assertFalse(s.removeIndex("_long"));

		//now try non-unique
		s.defineIndex("_long", false);
		assertTrue(s.isIndexDefined("_long"));
		assertFalse(s.isIndexUnique("_long"));

		//remove again
		s.removeIndex("_long");
		assertFalse(s.isIndexDefined("_long"));
		assertFalse(s.removeIndex("_long"));
		
		TestTools.closePM(pm);
	}

	
	@Test
	public void testIndexCreationWithExitingObjects() {
		createData();
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Schema s = Schema.locate(pm, TestClass.class, DB_NAME);

		s.defineIndex("_long", true);
		
		try {
			//re-create existing index
			s.defineIndex("_long", true);
			fail("Should have failed");
		} catch (JDOUserException e) {
			//good
		}
		
		try {
			//re-create existing index
			s.defineIndex("_long", false);
			fail("Should have failed");
		} catch (JDOUserException e) {
			//good
		}
		
		assertTrue(s.isIndexDefined("_long"));
		assertTrue(s.isIndexUnique("_long"));
		
		//remove
		assertTrue(s.removeIndex("_long"));
		assertFalse(s.isIndexDefined("_long"));
		assertFalse(s.removeIndex("_long"));

		//now try non-unique
		s.defineIndex("_long", false);
		assertTrue(s.isIndexDefined("_long"));
		assertFalse(s.isIndexUnique("_long"));

		//remove again
		assertTrue(s.removeIndex("_long"));
		assertFalse(s.isIndexDefined("_long"));
		assertFalse(s.removeIndex("_long"));
		
		TestTools.closePM(pm);
	}

	
	private void createData() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		TestClass tc1 = new TestClass();
		tc1.setData(1, false, 'c', (byte)127, (short)32000, 1234567890l, "xyz", new byte[]{1,2});
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(12, false, 'd', (byte)127, (short)32000, 1234567890l, "xyz", new byte[]{1,2});
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(123, false, 'e', (byte)127, (short)32000, 1234567890l, "xyz", new byte[]{1,2});
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(1234, false, 'f', (byte)127, (short)32000, 1234567890l, "xyz", new byte[]{1,2});
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(12345, false, 'g', (byte)127, (short)32000, 1234567890l, "xyz", new byte[]{1,2});
		pm.makePersistent(tc1);
		
		pm.currentTransaction().commit();
		TestTools.closePM();;
	}
	
	@After
	public void afterTest() {
		TestTools.closePM();
		TestTools.removeDb(DB_NAME);
	}
}

package org.zoodb.test;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.ZooClass;
import org.zoodb.test.util.TestTools;

public class Test_090_IndexManagement {

	@Before
	public void setUp() {
        TestTools.removeDb();
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
	}

	@Test
	public void testWithFreshIndex() {
		//fresh DB w/o index
        TestTools.removeDb();
		TestTools.createDb();

		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		ZooClass s = ZooClass.define(pm, TestClass.class);
		s.defineIndex("_long", true);
		assertTrue(s.isIndexDefined("_long"));

		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();

		s = ZooClass.define(pm, TestClass.class);
		assertFalse(s.isIndexDefined("_long"));
		s.defineIndex("_long", true);

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		assertTrue(s.removeIndex("_long"));
		s.defineIndex("_long", true);
		assertTrue(s.removeIndex("_long"));
		
		pm.currentTransaction().commit();
		TestTools.closePM(pm);
	}

	
	/**
	 * This should check whether the processing of the schema operation queue works correctly.
	 */
	@Test
	public void testTransient() {
		//fresh DB w/o index
        TestTools.removeDb();
		TestTools.createDb();

		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		ZooClass s = ZooClass.define(pm, TestClass.class);
		s.defineIndex("_long", true);
		assertTrue(s.removeIndex("_long"));
		s.remove();
		//TODO remove class as well in same transaction
		
		pm.currentTransaction().commit();
		TestTools.closePM(pm);
	}

	
	@Test
	public void testNonExistentField() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		ZooClass s = ZooClass.locate(pm, TestClass.class);
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

		ZooClass s = ZooClass.locate(pm, TestClass.class);

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
	public void testIndexCreationWithTx() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		ZooClass s = ZooClass.locate(pm, TestClass.class);
		s.defineIndex("_long", true);
		assertTrue(s.isIndexDefined("_long"));
		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();

		assertFalse(s.isIndexDefined("_long"));
		s.defineIndex("_long", true);
		assertTrue(s.isIndexDefined("_long"));
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		assertTrue(s.isIndexDefined("_long"));

		pm.currentTransaction().rollback();
		TestTools.closePM();
		
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		s = ZooClass.locate(pm, TestClass.class);
		assertTrue(s.isIndexDefined("_long"));

		try {
			//re-create existing index
			s.defineIndex("_long", true);
			fail("Should have failed");
		} catch (JDOUserException e) {
			//good
		}
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}

	@Test
	public void testIndexCreation() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		ZooClass s = ZooClass.locate(pm, TestClass.class);

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

		ZooClass s = ZooClass.locate(pm, TestClass.class);

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
		tc1.setData(1, false, 'c', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2});
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(12, false, 'd', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2});
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(123, false, 'e', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2});
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(1234, false, 'f', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2});
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(12345, false, 'g', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2});
		pm.makePersistent(tc1);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
	@Test
	public void testIndexCreationWithDifferentTypes() {
		createData();
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		ZooClass s = ZooClass.locate(pm, TestClass.class);

		s.defineIndex("_int", false);
		s.defineIndex("_long", false);
		s.defineIndex("_char", false);
		s.defineIndex("_byte", false);
		s.defineIndex("_short", false);
		s.defineIndex("_string", false);
		// not indexable
		checkThatDefinitionFails(pm, s, "_bool");
		// array of primitive
		checkThatDefinitionFails(pm, s, "_bArray");
		// object
		checkThatDefinitionFails(pm, s, "_intObj");
		// static primitive
		checkThatDefinitionFails(pm, s, "_staticInt");
		// static string 
		checkThatDefinitionFails(pm, s, "_staticString");
		// transient primitive
		checkThatDefinitionFails(pm, s, "_transientInt");
		// transient string 
		checkThatDefinitionFails(pm, s, "_transientString");
		// object ref
		checkThatDefinitionFails(pm, s, "_object");
		//object/pers ref
		checkThatDefinitionFails(pm, s, "_ref1");
		//persistent ref
		checkThatDefinitionFails(pm, s, "_ref2");
		
		TestTools.closePM(pm);
	}

	
	private void checkThatDefinitionFails(PersistenceManager pm, ZooClass s, String name) {
		try {
			//re-create existing index
			s.defineIndex(name, true);
			pm.currentTransaction().commit();
			fail("Should have failed: " + name);
		} catch (JDOUserException e) {
			//good
		}
	}

	@After
	public void afterTest() {
		TestTools.closePM();
		TestTools.removeDb();
	}
}

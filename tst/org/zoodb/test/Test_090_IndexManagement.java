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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.test.testutil.TestTools;

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

		ZooClass s = ZooSchema.defineClass(pm, TestClass.class);
		s.createIndex("_long", true);
		assertTrue(s.hasIndex("_long"));

		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();

		s = ZooSchema.defineClass(pm, TestClass.class);
		assertFalse(s.hasIndex("_long"));
		s.createIndex("_long", true);

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		assertTrue(s.removeIndex("_long"));
		s.createIndex("_long", true);
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

		ZooClass s = ZooSchema.defineClass(pm, TestClass.class);
		s.createIndex("_long", true);
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

		ZooClass s = ZooSchema.locateClass(pm, TestClass.class);
		try {
			//non-existent field
			s.removeIndex("xy!!z");
			fail("Should have failed");
		} catch (IllegalArgumentException e) {
			//good
		}
		
		try {
			//non-existent field
			s.hasIndex("xy!!z");
			fail("Should have failed");
		} catch (IllegalArgumentException e) {
			//good
		}
		
		try {
			//non-existent field
			s.isIndexUnique("xy!!z");
			fail("Should have failed");
		} catch (IllegalArgumentException e) {
			//good
		}
		
		try {
			//non-existent field
			s.createIndex("xy!!z", true);
			fail("Should have failed");
		} catch (IllegalArgumentException e) {
			//good
		}
		
		try {
			//re-create existing index
			s.createIndex("xy!!z", false);
			fail("Should have failed");
		} catch (IllegalArgumentException e) {
			//good
		}
		
		try {
			//non-existent field
			s.removeIndex("xy!!z");
			fail("Should have failed");
		} catch (IllegalArgumentException e) {
			//good
		}
		TestTools.closePM(pm);
	}

	
	@Test
	public void testNonExistentIndex() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		ZooClass s = ZooSchema.locateClass(pm, TestClass.class);

		//non-existent index
		assertFalse(s.removeIndex("_long"));
		
		try {
			//non-existent index
			s.isIndexUnique("_long");
			fail("Should have failed");
		} catch (IllegalStateException e) {
			//good
		}

		assertFalse(s.hasIndex("_long"));
		
		TestTools.closePM(pm);
	}

	
	@Test
	public void testIndexCreationWithTx() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		ZooClass s = ZooSchema.locateClass(pm, TestClass.class);
		s.createIndex("_long", true);
		assertTrue(s.hasIndex("_long"));
		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();

		assertFalse(s.hasIndex("_long"));
		s.createIndex("_long", true);
		assertTrue(s.hasIndex("_long"));
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		assertTrue(s.hasIndex("_long"));

		pm.currentTransaction().rollback();
		TestTools.closePM();
		
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		s = ZooSchema.locateClass(pm, TestClass.class);
		assertTrue(s.hasIndex("_long"));

		try {
			//re-create existing index
			s.createIndex("_long", true);
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

		ZooClass s = ZooSchema.locateClass(pm, TestClass.class);

		s.createIndex("_long", true);
		
		try {
			//re-create existing index
			s.createIndex("_long", true);
			fail("Should have failed");
		} catch (JDOUserException e) {
			//good
		}
		
		try {
			//re-create existing index
			s.createIndex("_long", false);
			fail("Should have failed");
		} catch (JDOUserException e) {
			//good
		}
		
		assertTrue(s.hasIndex("_long"));
		assertTrue(s.isIndexUnique("_long"));
		
		//remove
		assertTrue(s.removeIndex("_long"));
		assertFalse(s.hasIndex("_long"));
		assertFalse(s.removeIndex("_long"));

		//now try non-unique
		s.createIndex("_long", false);
		assertTrue(s.hasIndex("_long"));
		assertFalse(s.isIndexUnique("_long"));

		//remove again
		s.removeIndex("_long");
		assertFalse(s.hasIndex("_long"));
		assertFalse(s.removeIndex("_long"));
		
		TestTools.closePM(pm);
	}

	
	@Test
	public void testIndexCreationWithExitingObjects() {
		createData();
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		ZooClass s = ZooSchema.locateClass(pm, TestClass.class);

		s.createIndex("_long", true);
		
		try {
			//re-create existing index
			s.createIndex("_long", true);
			fail("Should have failed");
		} catch (JDOUserException e) {
			//good
		}
		
		try {
			//re-create existing index
			s.createIndex("_long", false);
			fail("Should have failed");
		} catch (JDOUserException e) {
			//good
		}
		
		assertTrue(s.hasIndex("_long"));
		assertTrue(s.isIndexUnique("_long"));
		
		//remove
		assertTrue(s.removeIndex("_long"));
		assertFalse(s.hasIndex("_long"));
		assertFalse(s.removeIndex("_long"));

		//now try non-unique
		s.createIndex("_long", false);
		assertTrue(s.hasIndex("_long"));
		assertFalse(s.isIndexUnique("_long"));

		//remove again
		assertTrue(s.removeIndex("_long"));
		assertFalse(s.hasIndex("_long"));
		assertFalse(s.removeIndex("_long"));
		
		TestTools.closePM(pm);
	}

	
	private void createData() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		TestClass tc1 = new TestClass();
		tc1.setData(1, false, 'c', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
				-1.1f, 35);
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(12, false, 'd', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
				-0.1f, 3);
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(123, false, 'e', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
				0.f, -3.5);
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(1234, false, 'f', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
				1.1f, -35);
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(12345, false, 'g', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
				21.1f, -335);
		pm.makePersistent(tc1);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
	@Test
	public void testIndexCreationWithDifferentTypes() {
		createData();
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		ZooClass s = ZooSchema.locateClass(pm, TestClass.class);

		s.createIndex("_int", false);
		s.createIndex("_long", false);
		s.createIndex("_char", false);
		s.createIndex("_byte", false);
		s.createIndex("_short", false);
		s.createIndex("_string", false);
		s.createIndex("_float", false);
		s.createIndex("_double", false);
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
			s.createIndex(name, true);
			pm.currentTransaction().commit();
			fail("Should have failed: " + name);
		} catch (IllegalArgumentException e) {
			//good
		}
	}

	@Test
	public void testIndexCreationUniqueOnNonUniqeData() {
		createData();
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		TestClass tc1 = new TestClass();
		tc1.setInt(200);
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setInt(200);
		pm.makePersistent(tc1);

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		ZooClass s = ZooSchema.locateClass(pm, TestClass.class);
		s.createIndex("_int", true);

		try {
			pm.currentTransaction().commit();
			fail();
		} catch (JDOUserException e) {
			//should fail because of non-unique data
		}
		
		TestTools.closePM(pm);
	}


	
	@After
	public void afterTest() {
		TestTools.closePM();
		TestTools.removeDb();
	}
}

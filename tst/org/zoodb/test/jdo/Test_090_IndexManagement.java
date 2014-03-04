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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.schema.ZooClass;
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

		ZooClass s = ZooJdoHelper.schema(pm).addClass(TestClass.class);
		s.createIndex("_long", true);
		assertTrue(s.hasIndex("_long"));

		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();

		s = ZooJdoHelper.schema(pm).addClass(TestClass.class);
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

		ZooClass s = ZooJdoHelper.schema(pm).addClass(TestClass.class);
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

		ZooClass s = ZooJdoHelper.schema(pm).getClass(TestClass.class);
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

		ZooClass s = ZooJdoHelper.schema(pm).getClass(TestClass.class);

		//non-existent index
		assertFalse(s.removeIndex("_long"));
		
		try {
			//non-existent index
			s.isIndexUnique("_long");
			fail("Should have failed");
		} catch (JDOUserException e) {
			//good
		}

		assertFalse(s.hasIndex("_long"));
		
		TestTools.closePM(pm);
	}

	
	@Test
	public void testIndexCreationWithTx() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		ZooClass s = ZooJdoHelper.schema(pm).getClass(TestClass.class);
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
		
		s = ZooJdoHelper.schema(pm).getClass(TestClass.class);
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

		ZooClass s = ZooJdoHelper.schema(pm).getClass(TestClass.class);

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

		ZooClass s = ZooJdoHelper.schema(pm).getClass(TestClass.class);

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

		ZooClass s = ZooJdoHelper.schema(pm).getClass(TestClass.class);

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
		
		ZooClass s = ZooJdoHelper.schema(pm).getClass(TestClass.class);
		s.createIndex("_int", true);

		try {
			pm.currentTransaction().commit();
			fail();
		} catch (JDOUserException e) {
			//should fail because of non-unique data
		}
		
		TestTools.closePM(pm);
	}

	@Test
	public void testIndexPropagationInClassHierarchy() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass s1 = ZooJdoHelper.schema(pm).addClass(TestClassTiny.class);
		ZooClass s2 = ZooJdoHelper.schema(pm).addClass(TestClassTiny2.class);

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		s1 = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s2 = ZooJdoHelper.schema(pm).getClass(TestClassTiny2.class);
		s1.createIndex("_long", true);
		s2.createIndex("_int", true);

		//rollback and do it again
		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();
		
		s1 = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s2 = ZooJdoHelper.schema(pm).getClass(TestClassTiny2.class);
		s1.createIndex("_long", true);
		s2.createIndex("_int", true);

		//close and reopen
		pm.currentTransaction().commit();
		TestTools.closePM();
		pm = TestTools.openPM();
		pm.currentTransaction().begin();

		//test modify super-class
		s1 = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s2 = ZooJdoHelper.schema(pm).getClass(TestClassTiny2.class);
		
		assertTrue(s1.hasIndex("_long"));
		//This is not possible, current policy is that indexing works only through declaring class.
		assertTrue(s2.hasIndex("_long"));
		assertTrue(s2.hasIndex("_int"));
		
		assertEquals(Arrays.toString(s1.getSubClasses().toArray()), 1, s1.getSubClasses().size());
		assertEquals(1, s1.getSubClasses().size());
		assertEquals(0, s2.getSubClasses().size());
		
		try {
			Query q = pm.newQuery("SELECT FROM " + s1.getName() + " WHERE _long1 > 0");
			Collection<?> c = (Collection<?>) q.execute();
			assertEquals(0, c.size());
		} catch (JDOUserException e) {
			//good, class not found, can not be materialised
		}
		
		Iterator<?> it = s1.getInstanceIterator();
		assertFalse(it.hasNext());
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}


	
	@After
	public void afterTest() {
		TestTools.closePM();
		TestTools.removeDb();
	}
}

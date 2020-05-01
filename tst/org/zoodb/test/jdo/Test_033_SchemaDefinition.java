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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import javax.jdo.Extent;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.schema.ZooClass;
import org.zoodb.schema.ZooField;
import org.zoodb.test.testutil.TestTools;
import org.zoodb.tools.DBStatistics.STATS;

public class Test_033_SchemaDefinition {

	private static final int SCHEMA_COUNT = 5; //Schema count on empty database
	
	@AfterClass
	public static void tearDown() {
	    TestTools.closePM();
	}

	@Before
	public void before() {
		TestTools.createDb();
	}
	
	@After
	public void after() {
		try {
			TestTools.closePM();
		} catch (Throwable t) {
			t.printStackTrace();
		} 
		TestTools.removeDb();
	}

	
	@Test
	public void testDeclareCommit() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass s = ZooJdoHelper.schema(pm).defineEmptyClass("MyClass");
		assertNotNull(s);
		assertEquals("MyClass", s.getName());
		ZooClass s2 = ZooJdoHelper.schema(pm).getClass("MyClass");
		assertNotNull(s2);
		pm.currentTransaction().commit();
		
		pm.currentTransaction().begin();
		ZooClass s3 = ZooJdoHelper.schema(pm).getClass("MyClass");
		assertNotNull(s3);
		pm.currentTransaction().rollback();
		TestTools.closePM();
		
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass s4 = ZooJdoHelper.schema(pm).getClass("MyClass");
		assertNotNull(s4);
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}

	
	@Test
	public void testDeclareAbort() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass s = ZooJdoHelper.schema(pm).defineEmptyClass("MyClass");
		assertNotNull(s);
		assertEquals("MyClass", s.getName());
		pm.currentTransaction().rollback();
		
		//try again
		pm.currentTransaction().begin();
		ZooClass s2 = ZooJdoHelper.schema(pm).getClass("MyClass");
		assertNull(s2);
		ZooClass s3 = ZooJdoHelper.schema(pm).defineEmptyClass("MyClass");
		assertNotNull(s3);
		pm.currentTransaction().rollback();
		TestTools.closePM();
		
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass s4 = ZooJdoHelper.schema(pm).getClass("MyClass");
		assertNull(s4);
		ZooClass s5 = ZooJdoHelper.schema(pm).defineEmptyClass("MyClass");
		assertNotNull(s5);
		pm.currentTransaction().commit();
		TestTools.closePM();
	}

	
	@Test
	public void testDeclareFails() {
		TestTools.defineSchema(TestClassTiny.class);
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		try {
			ZooJdoHelper.schema(pm).defineEmptyClass(TestClassTiny.class.getName());
			fail();
		} catch (IllegalArgumentException e) {
			//good, class exists
		}
		try {
			ZooJdoHelper.schema(pm).defineEmptyClass("");
			fail();
		} catch (IllegalArgumentException e) {
			//good, bad name
		}
		try {
			ZooJdoHelper.schema(pm).defineEmptyClass("1342dfs");
			fail();
		} catch (IllegalArgumentException e) {
			//good, bad name
		}
		try {
			ZooJdoHelper.schema(pm).defineEmptyClass(null);
			fail();
		} catch (IllegalArgumentException e) {
			//good, bad name
		}
		try {
			ZooJdoHelper.schema(pm).defineEmptyClass(String.class.getName());
			fail();
		} catch (IllegalArgumentException e) {
			//good, non-pers
		}

		TestTools.closePM();
		
		try {
			ZooJdoHelper.schema(pm).defineEmptyClass(TestClassTiny2.class.getName());
			fail();
		} catch (IllegalStateException e) {
			//good, outside session
		}
	}

	
	@Test
	public void testDeclareHierarchy() {
		TestTools.defineSchema(TestClassTiny.class);
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		ZooClass s1 = ZooJdoHelper.schema(pm).defineEmptyClass(cName1, stt);
		ZooClass s2 = ZooJdoHelper.schema(pm).defineEmptyClass(cName2, s1);
		assertEquals(stt, s1.getSuperClass());
		assertEquals(s1, s2.getSuperClass());
		pm.currentTransaction().rollback();
		
		//try again
		pm.currentTransaction().begin();
		stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s1 = ZooJdoHelper.schema(pm).defineEmptyClass(cName1, stt);
		s2 = ZooJdoHelper.schema(pm).defineEmptyClass(cName2, s1);
		assertEquals(stt, s1.getSuperClass());
		assertEquals(s1, s2.getSuperClass());
		pm.currentTransaction().commit();
		TestTools.closePM();

		//load and check again
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		assertEquals(stt, s1.getSuperClass());
		assertEquals(s1, s2.getSuperClass());
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}

	
	@Test
	public void testRemoveClassRollback() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass s1 = ZooJdoHelper.schema(pm).defineEmptyClass("MyClass");
		s1.remove();
		
		ZooClass s2 = ZooJdoHelper.schema(pm).getClass("MyClass");
		assertNull(s2);
		
		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();
		
		ZooClass s3 = ZooJdoHelper.schema(pm).getClass("MyClass");
		assertNull(s3);
		ZooClass s4 = ZooJdoHelper.schema(pm).defineEmptyClass("MyClass");
		assertNotNull(s4);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		ZooClass s5 = ZooJdoHelper.schema(pm).getClass("MyClass");
		assertNotNull(s5);
		s5.remove();

		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();
		
		ZooClass s6 = ZooJdoHelper.schema(pm).getClass("MyClass");
		assertNotNull(s6);
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}

	
	public void testRemoveClassCommit() {
		PersistenceManager pm = TestTools.openPM();
		
		//delete uncommitted
		pm.currentTransaction().begin();
		ZooClass s1 = ZooJdoHelper.schema(pm).defineEmptyClass("MyClass");
		s1.remove();

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		//delete committed
		ZooClass s3 = ZooJdoHelper.schema(pm).getClass("MyClass");
		assertNull(s3);
		ZooClass s4 = ZooJdoHelper.schema(pm).defineEmptyClass("MyClass");
		assertNotNull(s4);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		ZooClass s5 = ZooJdoHelper.schema(pm).getClass("MyClass");
		s5.remove();
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		ZooClass s6 = ZooJdoHelper.schema(pm).getClass("MyClass");
		assertNull(s6);

		pm.currentTransaction().rollback();
		TestTools.closePM();
	}

	
	@Test
	public void testRemoveFails() {
		TestTools.defineSchema(TestClassTiny.class);
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		//remove uncommitted
		ZooClass s1 = ZooJdoHelper.schema(pm).defineEmptyClass("MyClass");
		s1.remove();
		try {
			s1.remove();
			fail();
		} catch (IllegalStateException e) {
			//good
		}

		//remove committed
		ZooClass s2 = ZooJdoHelper.schema(pm).defineEmptyClass("MyClass2");
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		s2 = ZooJdoHelper.schema(pm).getClass("MyClass2");
		s2.remove();
		try {
			s2.remove();
			fail();
		} catch (IllegalStateException e) {
			//good
		}

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		s2 = ZooJdoHelper.schema(pm).getClass("MyClass2");
		assertNull(s2);

		pm.currentTransaction().rollback();
		TestTools.closePM();
	}

	
	@Test
	public void testRemoveHierarchy() {
		TestTools.defineSchema(TestClassTiny.class);
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		ZooClass s1 = ZooJdoHelper.schema(pm).defineEmptyClass(cName1, stt);
		ZooClass s2 = ZooJdoHelper.schema(pm).defineEmptyClass(cName2, s1);
		s1.removeWithSubClasses();
		assertNull(ZooJdoHelper.schema(pm).getClass(cName1));
		assertNull(ZooJdoHelper.schema(pm).getClass(cName2));
		
		pm.currentTransaction().rollback();
		
		//try again, this time with commit
		pm.currentTransaction().begin();
		stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s1 = ZooJdoHelper.schema(pm).defineEmptyClass(cName1, stt);
		s2 = ZooJdoHelper.schema(pm).defineEmptyClass(cName2, s1);
		pm.currentTransaction().commit();
		TestTools.closePM();

		//load and check again
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		stt.removeWithSubClasses();
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		assertNull(s1);
		assertNull(s2);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		assertNull(stt);
		assertNull(s1);
		assertNull(s2);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}

	@Test
	public void testLocateClass() {
		TestTools.defineSchema(TestClassTiny.class);
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		assertNull(ZooJdoHelper.schema(pm).getClass(String.class));
		assertNull(ZooJdoHelper.schema(pm).getClass((Class<?>)null));
		assertNull(ZooJdoHelper.schema(pm).getClass((String)null));
		assertNull(ZooJdoHelper.schema(pm).getClass(""));
		assertNull(ZooJdoHelper.schema(pm).getClass("  %% "));
		
		ZooClass stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		ZooClass s1 = ZooJdoHelper.schema(pm).defineEmptyClass(cName1, stt);
		ZooClass s2 = ZooJdoHelper.schema(pm).defineEmptyClass(cName2, s1);
		assertTrue(stt == ZooJdoHelper.schema(pm).getClass(TestClassTiny.class));
		assertTrue(s1 == ZooJdoHelper.schema(pm).getClass(cName1));
		assertTrue(s2 == ZooJdoHelper.schema(pm).getClass(cName2));
		
		pm.currentTransaction().rollback();
		
		try {
			ZooJdoHelper.schema(pm).defineEmptyClass(cName1, stt);
			fail();
		} catch(IllegalStateException e) {
			//good, pm is closed!
		}
		
		TestTools.closePM();
		
		try {
			ZooJdoHelper.schema(pm).defineEmptyClass(cName1, stt);
			fail();
		} catch(IllegalStateException e) {
			//good, pm is closed!
		}
	}	
	
	
	@Test
	public void testGetAttribute() {
		TestTools.defineSchema(TestClassTiny.class);
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		ZooClass s1 = ZooJdoHelper.schema(pm).defineEmptyClass(cName1, stt);
		ZooClass s2 = ZooJdoHelper.schema(pm).defineEmptyClass(cName2, s1);
		assertTrue(s1.getAllFields().size() == 2);
		assertTrue(s1.getLocalFields().size() == 0);
		assertTrue(s2.getAllFields().size() == 2);
		assertTrue(s2.getLocalFields().size() == 0);
		
		assertNull(s1.getField("_int1"));
		assertNotNull(s1.getField("_int"));
		assertNotNull(s2.getField("_long"));
		
		s1.addField("_int1", Integer.TYPE);
		s1.addField("_long1", Long.TYPE);
		s2.addField("ref1", s1, 0);
		s2.addField("ref1Array", s1, 2);

		//check local fields
		checkFields(s1.getLocalFields(), "_int1", "_long1");
		checkFields(s2.getLocalFields(), "ref1", "ref1Array");
		//check all fields 
		checkFields(s1.getAllFields(), "_int", "_long", "_int1", "_long1");
		checkFields(s2.getAllFields(), "_int", "_long", "_int1", "_long1", "ref1", "ref1Array");		
		
		pm.currentTransaction().commit();
		
		//try again
		pm.currentTransaction().begin();
		stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		
		//check 1st class
		//check local fields
		checkFields(s1.getLocalFields(), "_int1", "_long1");
		checkFields(s2.getLocalFields(), "ref1", "ref1Array");

		//check all fields 
		checkFields(s1.getAllFields(), "_int", "_long", "_int1", "_long1");
		checkFields(s2.getAllFields(), "_int", "_long", "_int1", "_long1", "ref1", "ref1Array");		

		pm.currentTransaction().commit();
		TestTools.closePM();
		
		try {
			s1.getAllFields();
			fail();
		} catch (IllegalStateException e) {
			//good, pm is closed
		}
		try {
			s1.getLocalFields();
			fail();
		} catch (IllegalStateException e) {
			//good, pm is closed
		}
	}

	
	@Test
	public void testAddAttribute() {
		TestTools.defineSchema(TestClassTiny.class);
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		ZooClass s1 = ZooJdoHelper.schema(pm).defineEmptyClass(cName1, stt);
		ZooClass s2 = ZooJdoHelper.schema(pm).defineEmptyClass(cName2, s1);
		
		s1.addField("_int1", Integer.TYPE);
		s1.addField("_long1", Long.TYPE);
		s2.addField("ref1", s1, 0);
		s2.addField("ref1Array", s1, 2);

		//check local fields
		checkFields(s1.getLocalFields(), "_int1", "_long1");
		checkFields(s2.getLocalFields(), "ref1", "ref1Array");
		
		pm.currentTransaction().commit();
		
		//try again
		pm.currentTransaction().begin();
		stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		
		//check local fields
		checkFields(s1.getLocalFields(), "_int1", "_long1");
		checkFields(s2.getLocalFields(), "ref1", "ref1Array");

		pm.currentTransaction().commit();
		TestTools.closePM();

		//load and check again
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		
		checkFields(s1.getLocalFields(), "_int1", "_long1");
		checkFields(s2.getLocalFields(), "ref1", "ref1Array");
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}

	@Test
	public void testAddAttributeRollback() {
		TestTools.defineSchema(TestClassTiny.class);
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		ZooClass s1 = ZooJdoHelper.schema(pm).defineEmptyClass(cName1, stt);
		ZooClass s2 = ZooJdoHelper.schema(pm).defineEmptyClass(cName2, s1);

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		s1.addField("_int1", Integer.TYPE);
		s2.addField("ref1", s1, 0);

		//check local fields
		checkFields(s1.getLocalFields(), "_int1");
		checkFields(s2.getLocalFields(), "ref1");
		
		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();

		stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		
		//check local fields
		checkFields(s1.getLocalFields());
		checkFields(s2.getLocalFields());
	}

	@Test
	public void testAddAttributeFails() {
		TestTools.defineSchema(TestClassTiny.class);
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		ZooClass s1 = ZooJdoHelper.schema(pm).defineEmptyClass(cName1, stt);
		ZooClass s2 = ZooJdoHelper.schema(pm).defineEmptyClass(cName2, s1);
		
		s1.addField("_int1", Integer.TYPE);
		s1.addField("_long1", Long.TYPE);
		s2.addField("ref1", s1, 0);
		s2.addField("ref1Array", s1, 2);

		try {
			s1.addField("_long", Long.TYPE);
			fail();
		} catch (IllegalArgumentException e) {
			//good, this name is already taken...
		}
		try {
			s1.addField("_long1", Long.TYPE);
			fail();
		} catch (IllegalArgumentException e) {
			//good, this name is already taken...
		}
		try {
			s1.addField(null, Long.TYPE);
			fail();
		} catch (IllegalArgumentException e) {
			//good, this name is invalid...
		}
		try {
			s1.addField("", Long.TYPE);
			fail();
		} catch (IllegalArgumentException e) {
			//good, this name is invalid...
		}
		try {
			s1.addField("1_long1", Long.TYPE);
			fail();
		} catch (IllegalArgumentException e) {
			//good, this name is invalid...
		}
		try {
			s1.addField("MyClass.x", Long.TYPE);
			fail();
		} catch (IllegalArgumentException e) {
			//good, this name is invalid...
		}

		try {
			s1.addField("1_long1", null);
			fail();
		} catch (IllegalArgumentException e) {
			//good, this type is invalid...
		}

		
		//check local fields
		checkFields(s1.getLocalFields(), "_int1", "_long1");
		checkFields(s2.getLocalFields(), "ref1", "ref1Array");
		
		pm.currentTransaction().commit();

		TestTools.closePM();

		try {
			s1.addField("xyz", Long.TYPE);
			fail();
		} catch (IllegalStateException e) {
			//good, pm is closed
		}
		
		try {
			s1.addField("xyz2", Long.TYPE);
			fail();
		} catch (IllegalStateException e) {
			//good, pm is closed
		}
	}

	@Test
	public void testRenameAttribute() {
		TestTools.defineSchema(TestClassTiny.class);
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		ZooClass s1 = ZooJdoHelper.schema(pm).defineEmptyClass(cName1, stt);
		ZooClass s2 = ZooJdoHelper.schema(pm).defineEmptyClass(cName2, s1);
		
		ZooField f11 = s1.addField("_int1", Integer.TYPE);
		ZooField f12 = s1.addField("_long1", Long.TYPE);
		ZooField f21 = s2.addField("ref1", s1, 0);
		ZooField f22 = s2.addField("ref1Array", s1, 2);

		f11.rename("_int11");
		try {
			f12.rename("_long");
		} catch (IllegalArgumentException e) {
			//good, this name is already taken...
		}
		try {
			f21.rename("");
		} catch (IllegalArgumentException e) {
			//good, this name is invalid...
		}
		try {
			f22.rename("123_dhsak");
		} catch (IllegalArgumentException e) {
			//good, this name is invalid...
		}
		
		//check local fields
		checkFields(s1.getLocalFields(), "_int11", "_long1");
		checkFields(s2.getAllFields(), "_int", "_long", "_int11", "_long1", "ref1", "ref1Array");
		
		pm.currentTransaction().commit();
		
		//try again
		pm.currentTransaction().begin();
		stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		
		//check local fields
		checkFields(s1.getLocalFields(), "_int11", "_long1");
		checkFields(s2.getAllFields(), "_int", "_long", "_int11", "_long1", "ref1", "ref1Array");

		s1.getField("_int11").rename("_int111");
		checkFields(s1.getLocalFields(), "_int111", "_long1");
		
		//rollback
		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();

		//check again
		checkFields(s1.getLocalFields(), "_int11", "_long1");
		checkFields(s2.getAllFields(), "_int", "_long", "_int11", "_long1", "ref1", "ref1Array");
		
		pm.currentTransaction().rollback();
		TestTools.closePM();

		//load and check again
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		
		checkFields(s1.getLocalFields(), "_int11", "_long1");
		checkFields(s2.getAllFields(), "_int", "_long", "_int11", "_long1", "ref1", "ref1Array");
		pm.currentTransaction().rollback();
		TestTools.closePM();
		
		try {
			s1.addField("xyz", Long.TYPE);
			fail();
		} catch (IllegalStateException e) {
			//good, pm is closed
		}
	}

	@Test
	public void testRemoveAttribute() {
		TestTools.defineSchema(TestClassTiny.class);
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		ZooClass s1 = ZooJdoHelper.schema(pm).defineEmptyClass(cName1, stt);
		ZooClass s2 = ZooJdoHelper.schema(pm).defineEmptyClass(cName2, s1);
		
		try {
			s2.addField("_int", Long.TYPE);
			fail();
		} catch (IllegalArgumentException e) {
			// good, field already exists in super-super class
		}
		
		ZooField f11 = s1.addField("_int1", Integer.TYPE);
		ZooField f12 = s1.addField("_long1", Long.TYPE);
		ZooField f13 = s1.addField("_long12", Long.TYPE);
		ZooField f21 = s2.addField("ref1", s1, 0);
		ZooField f22 = s2.addField("ref1Array", s1, 1);
		assertNotNull(f13);
		assertNotNull(f22);

		f11.remove();
		s1.removeField(s1.getField("_long12"));
		s2.removeField(f21.getName());
		
		List<ZooField> fields1 = s1.getLocalFields();
		assertTrue(fields1.get(0).getName() == "_long1");
		assertEquals(1, fields1.size());
		
		List<ZooField> fields2 = s2.getLocalFields();
		assertTrue(fields2.get(0).getName() == "ref1Array");
		assertEquals(1, fields2.size());

		checkFields(s1.getLocalFields(), "_long1");
		checkFields(s2.getLocalFields(), "ref1Array");
		
		checkFields(s1.getAllFields(), "_int", "_long", "_long1");
		checkFields(s2.getAllFields(), "_int", "_long", "_long1", "ref1Array");

		pm.currentTransaction().commit();
		TestTools.closePM();

		//load and check again
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		
		checkFields(s1.getAllFields(), "_int", "_long", "_long1");
		checkFields(s2.getAllFields(), "_int", "_long", "_long1", "ref1Array");

		pm.currentTransaction().rollback();
		TestTools.closePM();
		try {
			f12.remove();
			fail();
		} catch (IllegalStateException e) {
			//good, pm is closed
		}
	}

	@Test
	public void testSchemaCountAbort() {
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		checkSchemaCount(pm, 0);
		
		ZooClass stt = ZooJdoHelper.schema(pm).addClass(TestClassTiny.class);
		ZooClass s1 = ZooJdoHelper.schema(pm).defineEmptyClass(cName1, stt);
		ZooClass s2 = ZooJdoHelper.schema(pm).defineEmptyClass(cName2, s1);
		
		ZooField f1 = s2.addField("_int1", Integer.TYPE);
		f1.rename("_int1_1");
		f1.remove();
		
		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();
		
		checkSchemaCount(pm, 0);
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}
	
	@Test
	public void testSchemaCountCommit() {
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooJdoHelper.schema(pm).addClass(TestClassTiny.class);
		ZooClass s1 = ZooJdoHelper.schema(pm).defineEmptyClass(cName1, stt);
		ZooClass s2 = ZooJdoHelper.schema(pm).defineEmptyClass(cName2, s1);
		
		ZooField f1 = s1.addField("_long1", Long.TYPE);
		f1.rename("_long_1_1");
		
		ZooField f2 = s2.addField("_int1", Integer.TYPE);
		f2.rename("_int1_1");
		f2.remove();
		s2.remove();
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		checkSchemaCount(pm, 2);

		//test modify super-class
		stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		stt.addField("xyz", Long.TYPE);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		checkSchemaCount(pm, 4);  //class and sub-class have new attribute

		//test add
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s1.addField("xyz2", Long.TYPE);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		checkSchemaCount(pm, 5);  //class and sub-class have new attribute

		//test rename
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s1.getField("xyz2").rename("xyz3");
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		checkSchemaCount(pm, 5);  //renaming does not create new version

		//test remove
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s1.getField("xyz3").remove();
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		checkSchemaCount(pm, 6);  //class and sub-class have new attribute

		//test combo (should result in one change only)
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		f1 = s1.addField("aaa", Long.TYPE);
		f1.rename("aaa2");
		f2 = s1.addField("bbb", Long.TYPE);
		f2.rename("bbb2");
		f2.remove();
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		checkSchemaCount(pm, 7);  //class and sub-class have new attribute

		
		TestTools.closePM();
	}
	
	@Test
	public void testSchemaCountOpenClose() {
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooJdoHelper.schema(pm).addClass(TestClassTiny.class);
		ZooClass s1 = ZooJdoHelper.schema(pm).defineEmptyClass(cName1, stt);
		ZooClass s2 = ZooJdoHelper.schema(pm).defineEmptyClass(cName2, s1);
		
		ZooField f1 = s1.addField("_long1", Long.TYPE);
		f1.rename("_long_1_1");
		
		ZooField f2 = s2.addField("_int1", Integer.TYPE);
		f2.rename("_int1_1");
		f2.remove();
		s2.remove();
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		checkSchemaCount(pm, 2);

		//test modify super-class
		stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		stt.addField("xyz", Long.TYPE);
		pm.currentTransaction().commit();
		TestTools.closePM();
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		checkSchemaCount(pm, 4);  //class and sub-class have new attribute

		//test add
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s1.addField("xyz2", Long.TYPE);
		pm.currentTransaction().commit();
		TestTools.closePM();
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		checkSchemaCount(pm, 5);  //class and sub-class have new attribute

		//test rename
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s1.getField("xyz2").rename("xyz3");
		pm.currentTransaction().commit();
		TestTools.closePM();
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		checkSchemaCount(pm, 5);  //class and sub-class have new attribute

		//test remove
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s1.getField("xyz3").remove();
		pm.currentTransaction().commit();
		TestTools.closePM();
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		checkSchemaCount(pm, 6);  //class and sub-class have new attribute

		//test combo (should result in one change only)
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		f1 = s1.addField("aaa", Long.TYPE);
		f1.rename("aaa2");
		f2 = s1.addField("bbb", Long.TYPE);
		f2.rename("bbb2");
		f2.remove();
		pm.currentTransaction().commit();
		TestTools.closePM();
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		checkSchemaCount(pm, 7);  //class and sub-class have new attribute

		
		TestTools.closePM();
	}
	
	@Test
	public void testModifySubClassFirstWithClose() {
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooJdoHelper.schema(pm).addClass(TestClassTiny.class);
		ZooClass s1 = ZooJdoHelper.schema(pm).defineEmptyClass(cName1, stt);
		ZooClass s2 = ZooJdoHelper.schema(pm).defineEmptyClass(cName2, s1);
		
		s1.addField("_long1", Long.TYPE);
		s2.addField("_int1", Integer.TYPE);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		checkSchemaCount(pm, 3);

		//test modify super-class
		stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		s2.addField("_f22", Long.TYPE);
		s1.addField("_f12", Long.TYPE);
		stt.addField("xyz", Long.TYPE);
		checkSchemaCount(pm, 6);  //class and sub-class have new attribute
		checkFields(stt.getAllFields(), "_int", "_long", "xyz");
		checkFields(s1.getAllFields(), "_int", "_long", "xyz", "_long1", "_f12");
		checkFields(s2.getAllFields(), "_int", "_long", "xyz", "_long1", "_f12", "_int1", "_f22");
		
		pm.currentTransaction().commit();
		// try with closing session
		TestTools.closePM();
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		checkSchemaCount(pm, 6);  //class and sub-class have new attribute

		stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		assertEquals(1, stt.getSubClasses().size());
		assertEquals(1, s1.getSubClasses().size());
		assertEquals(0, s2.getSubClasses().size());
		
		checkFields(stt.getAllFields(), "_int", "_long", "xyz");
		checkFields(s1.getAllFields(), "_int", "_long", "xyz", "_long1", "_f12");
		checkFields(s2.getAllFields(), "_int", "_long", "xyz", "_long1", "_f12", "_int1", "_f22");
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
	@Test
	public void testModify2ndSubClassFirstWithClose() {
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooJdoHelper.schema(pm).addClass(TestClassTiny.class);
		ZooClass s1 = ZooJdoHelper.schema(pm).defineEmptyClass(cName1, stt);
		ZooClass s2 = ZooJdoHelper.schema(pm).defineEmptyClass(cName2, s1);
		
		s1.addField("_long1", Long.TYPE);
		s2.addField("_int1", Integer.TYPE);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		checkSchemaCount(pm, 3);

		//test modify super-class
		stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		s2.addField("_f22", Long.TYPE);
		stt.addField("xyz", Long.TYPE);
		checkSchemaCount(pm, 6);  //class and sub-class have new attribute
		checkFields(stt.getAllFields(), "_int", "_long", "xyz");
		checkFields(s1.getAllFields(), "_int", "_long", "xyz", "_long1");
		checkFields(s2.getAllFields(), "_int", "_long", "xyz", "_long1", "_int1", "_f22");
		
		pm.currentTransaction().commit();
		// try with closing session
		TestTools.closePM();
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		checkSchemaCount(pm, 6);  //class and sub-class have new attribute

		stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		assertEquals(1, stt.getSubClasses().size());
		assertEquals(1, s1.getSubClasses().size());
		assertEquals(0, s2.getSubClasses().size());
		
		checkFields(stt.getAllFields(), "_int", "_long", "xyz");
		checkFields(s1.getAllFields(), "_int", "_long", "xyz", "_long1");
		checkFields(s2.getAllFields(), "_int", "_long", "xyz", "_long1", "_int1", "_f22");
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
	@Test
	public void testModifySubClassFirstNoClose() {
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooJdoHelper.schema(pm).addClass(TestClassTiny.class);
		ZooClass s1 = ZooJdoHelper.schema(pm).defineEmptyClass(cName1, stt);
		ZooClass s2 = ZooJdoHelper.schema(pm).defineEmptyClass(cName2, s1);
		
		s1.addField("_long1", Long.TYPE);
		s2.addField("_int1", Integer.TYPE);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		checkSchemaCount(pm, 3);

		//test modify super-class
		stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		s2.addField("_f22", Long.TYPE);
		s1.addField("_f12", Long.TYPE);
		stt.addField("xyz", Long.TYPE);
		checkSchemaCount(pm, 6);  //class and sub-class have new attribute
		checkFields(stt.getAllFields(), "_int", "_long", "xyz");
		checkFields(s1.getAllFields(), "_int", "_long", "xyz", "_long1", "_f12");
		checkFields(s2.getAllFields(), "_int", "_long", "xyz", "_long1", "_f12", "_int1", "_f22");
		
		pm.currentTransaction().commit();
		// try with closing session
		pm.currentTransaction().begin();
		checkSchemaCount(pm, 6);  //class and sub-class have new attribute

		stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		assertEquals(1, stt.getSubClasses().size());
		assertEquals(1, s1.getSubClasses().size());
		assertEquals(0, s2.getSubClasses().size());
		
		checkFields(stt.getAllFields(), "_int", "_long", "xyz");
		checkFields(s1.getAllFields(), "_int", "_long", "xyz", "_long1", "_f12");
		checkFields(s2.getAllFields(), "_int", "_long", "xyz", "_long1", "_f12", "_int1", "_f22");
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
	@Test
	public void testIndexPropagation() {
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooJdoHelper.schema(pm).addClass(TestClassTiny.class);
		ZooClass s1 = ZooJdoHelper.schema(pm).defineEmptyClass(cName1, stt);
		ZooClass s2 = ZooJdoHelper.schema(pm).defineEmptyClass(cName2, s1);

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		checkSchemaCount(pm, 3);
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		s1.addField("_long1", Long.TYPE);
		s1.createIndex("_long1", true);
		s2.addField("_int1", Integer.TYPE);
		s2.createIndex("_int1", true);
		checkSchemaCount(pm, 5);

		//rollback and do it again
		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();
		
		checkSchemaCount(pm, 3);
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		s1.addField("_long1", Long.TYPE);
		s1.createIndex("_long1", true);
		s2.addField("_int1", Integer.TYPE);
		s2.createIndex("_int1", true);

		//close and reopen
		pm.currentTransaction().commit();
		TestTools.closePM();
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		checkSchemaCount(pm, 5);

		//test modify super-class
		stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		
		assertTrue(s1.hasIndex("_long1"));
		//This is not possible, current policy is that indexing works only through declaring class.
		assertTrue(s2.hasIndex("_long1"));
		assertTrue(s2.hasIndex("_int1"));
		
		assertEquals(Arrays.toString(stt.getSubClasses().toArray()), 1, stt.getSubClasses().size());
		assertEquals(1, s1.getSubClasses().size());
		assertEquals(0, s2.getSubClasses().size());
		
		try {
			Query q = pm.newQuery("SELECT FROM " + cName1 + " WHERE _long1 > 0");
			Collection<?> c = (Collection<?>) q.execute();
			assertEquals(0, c.size());
		} catch (JDOUserException e) {
			//good, class not found, cannot be materialised
		}
		
		Iterator<?> it = s1.getInstanceIterator();
		assertFalse(it.hasNext());
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}

	
	@Test
	public void testIndexPropagationViaFields() {
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooJdoHelper.schema(pm).addClass(TestClassTiny.class);
		ZooClass s1 = ZooJdoHelper.schema(pm).defineEmptyClass(cName1, stt);
		ZooClass s2 = ZooJdoHelper.schema(pm).defineEmptyClass(cName2, s1);

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		checkSchemaCount(pm, 3);
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		ZooField f1 = s1.addField("_long1", Long.TYPE);
		f1.createIndex(true);
		ZooField f2 = s2.addField("_int1", Integer.TYPE);
		f2.createIndex(true);
		checkSchemaCount(pm, 5);

		//rollback and do it again
		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();
		
		checkSchemaCount(pm, 3);
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		f1 = s1.addField("_long1", Long.TYPE);
		f1.createIndex(true);
		f2 = s2.addField("_int1", Integer.TYPE);
		f2.createIndex(false);

		//close and reopen
		pm.currentTransaction().commit();
		TestTools.closePM();
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		checkSchemaCount(pm, 5);

		//test modify super-class
		stt = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		
		assertTrue(s1.getField("_long1").hasIndex());
		//This is not possible, current policy is that indexing works only through declaring class.
		assertTrue(s2.getField("_long1").hasIndex());
		assertTrue(s2.getField("_long1").isIndexUnique());
		assertTrue(s2.getField("_int1").hasIndex());
		assertFalse(s2.getField("_int1").isIndexUnique());
		
		assertEquals(Arrays.toString(stt.getSubClasses().toArray()), 1, stt.getSubClasses().size());
		assertEquals(1, s1.getSubClasses().size());
		assertEquals(0, s2.getSubClasses().size());
		
		try {
			Query q = pm.newQuery("SELECT FROM " + cName1 + " WHERE _long1 > 0");
			Collection<?> c = (Collection<?>) q.execute();
			assertEquals(0, c.size());
		} catch (JDOUserException e) {
			//good, class not found, cannot be materialised
		}
		
		Iterator<?> it = s1.getInstanceIterator();
		assertFalse(it.hasNext());
		
		//delete index
		s1.getField("_long1").removeIndex();
		s2.getField("_int1").removeIndex();

		assertFalse(s1.getField("_long1").hasIndex());
		assertFalse(s2.getField("_long1").hasIndex());
		assertFalse(s2.getField("_int1").hasIndex());
		try {
			assertFalse(s2.getField("_int1").isIndexUnique());
			fail();
		} catch (JDOUserException e) {
			//no index defined
		}
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		assertFalse(s1.getField("_long1").hasIndex());
		assertFalse(s2.getField("_long1").hasIndex());
		assertFalse(s2.getField("_int1").hasIndex());
		try {
			assertFalse(s2.getField("_int1").isIndexUnique());
			fail();
		} catch (JDOUserException e) {
			//no index defined
		}
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}

	
	@Test
	public void testConstructClass() {
		String cName1 = TestClassTiny.class.getName();
		String cName2 = TestClassTiny2.class.getName();
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass s1 = ZooJdoHelper.schema(pm).defineEmptyClass(cName1);
		ZooClass s2 = ZooJdoHelper.schema(pm).defineEmptyClass(cName2, s1);

		//interim commit tests that the Java class is associated only with the LATEST version,
		//other versions are usually incompatible.
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		s1 = ZooJdoHelper.schema(pm).getClass(cName1);
		s1.addField("_int", Integer.TYPE);
		//interleave field creation with sub-class creation
		s2 = ZooJdoHelper.schema(pm).getClass(cName2);
		s1.addField("_long", Long.TYPE);

		//just for fun, trying reverse order...
		s2.addField("l2", Long.TYPE);
		s2.addField("i2", Integer.TYPE);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClassTiny t1 = new TestClassTiny(1,2);
		TestClassTiny2 t2 = new TestClassTiny2(3,4,5,6);
		pm.makePersistent(t1);
		pm.makePersistent(t2);
		Object oid1 = pm.getObjectId(t1);
		Object oid2 = pm.getObjectId(t2);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//query
		Query q = pm.newQuery("SELECT FROM " + cName1 + " WHERE _long > 0");
		Collection<?> c = (Collection<?>) q.execute();
		assertEquals(2, c.size());
		q.close(c);
		
		//extent
		Extent<TestClassTiny> ex1 = pm.getExtent(TestClassTiny.class, true);
		Iterator<TestClassTiny> ei = ex1.iterator();
		int n = 0;
		while (ei.hasNext()) {
			TestClassTiny tct = ei.next();
			assertTrue(tct.getInt() > 0);
			n++;
		}
		assertEquals(2, n);
		ex1.closeAll();
		
		//oids
		t1 = (TestClassTiny) pm.getObjectById(oid1);
		assertEquals(2, t1.getLong());
		t2 = (TestClassTiny2) pm.getObjectById(oid2);
		assertEquals(4, t2.getLong());
		assertEquals(6, t2.getLong2());
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
	private void checkFields(List<ZooField> list, String ...names) {
		for (int i = 0; i < names.length; i++) {
			assertEquals(names[i], list.get(i).getName());
		}
		assertEquals(names.length, list.size());
	}
	
	private void checkSchemaCount(PersistenceManager pm, int expected) {
		Extent<?> e = pm.getExtent(ZooClassDef.class);
		int n = 0;
		for (Object o: e) {
			assertNotNull(o);
			n++;
		}
		assertEquals(SCHEMA_COUNT + expected, n);
	}
	
	

	@Test
	public void testSchemaHierarchyPageUse() {
		//test that allocating 10 schemas does not require too many pages
		int N = 10;
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		ZooClass cls = ZooJdoHelper.schema(pm).defineEmptyClass("Sub"); 
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		long pageCount1 = ZooJdoHelper.getStatistics(pm).getStat(STATS.DB_PAGE_CNT);
		
		for (int i = 0; i < 10; i++) {
			cls = ZooJdoHelper.schema(pm).defineEmptyClass("Sub" + i, cls);
		}
		
		pm.currentTransaction().commit();
		long pageCount2 = ZooJdoHelper.getStatistics(pm).getStat(STATS.DB_PAGE_CNT);
		TestTools.closePM();
		
		assertTrue("n1 = " + pageCount1 + "  n2 = " + pageCount2, pageCount2 <= pageCount1 + N + 2);
	}
}

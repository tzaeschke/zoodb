/*
 * Copyright 2009-2012 Tilmann Zäschke. All rights reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.api.ZooField;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.test.util.TestTools;

public class Test_033_SchemaDefinition {

	@AfterClass
	public static void tearDown() {
	    TestTools.closePM();
	}

	@Before
	public void before() {
		//TestTools.removeDb();
		TestTools.createDb();
	}
	
	@After
	public void after() {
		TestTools.closePM();
		TestTools.removeDb();
	}

	
	@Test
	public void testDeclareCommit() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass s = ZooSchema.declareClass(pm, "MyClass");
		assertNotNull(s);
		assertEquals("MyClass", s.getClassName());
		ZooClass s2 = ZooSchema.locateClass(pm, "MyClass");
		assertNotNull(s2);
		pm.currentTransaction().commit();
		
		pm.currentTransaction().begin();
		ZooClass s3 = ZooSchema.locateClass(pm, "MyClass");
		assertNotNull(s3);
		pm.currentTransaction().rollback();
		TestTools.closePM();
		
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass s4 = ZooSchema.locateClass(pm, "MyClass");
		assertNotNull(s4);
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}

	
	@Test
	public void testDeclareAbort() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass s = ZooSchema.declareClass(pm, "MyClass");
		assertNotNull(s);
		assertEquals("MyClass", s.getClassName());
		pm.currentTransaction().rollback();
		
		//try again
		pm.currentTransaction().begin();
		ZooClass s2 = ZooSchema.locateClass(pm, "MyClass");
		assertNull(s2);
		ZooClass s3 = ZooSchema.declareClass(pm, "MyClass");
		assertNotNull(s3);
		pm.currentTransaction().rollback();
		TestTools.closePM();
		
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass s4 = ZooSchema.locateClass(pm, "MyClass");
		assertNull(s4);
		ZooClass s5 = ZooSchema.declareClass(pm, "MyClass");
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
			ZooSchema.declareClass(pm, TestClassTiny.class.getName());
			fail();
		} catch (JDOUserException e) {
			//good
		}
		TestTools.closePM();
	}

	
	@Test
	public void testDeclareHierarchy() {
		TestTools.defineSchema(TestClassTiny.class);
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		ZooClass s1 = ZooSchema.declareClass(pm, cName1, stt);
		ZooClass s2 = ZooSchema.declareClass(pm, cName2, s1);
		assertEquals(stt, s1.getSuperClass());
		assertEquals(s1, s2.getSuperClass());
		pm.currentTransaction().rollback();
		
		//try again
		pm.currentTransaction().begin();
		stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		s1 = ZooSchema.declareClass(pm, cName1, stt);
		s2 = ZooSchema.declareClass(pm, cName2, s1);
		assertEquals(stt, s1.getSuperClass());
		assertEquals(s1, s2.getSuperClass());
		pm.currentTransaction().commit();
		TestTools.closePM();

		//load and check again
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		s1 = ZooSchema.locateClass(pm, cName1);
		s2 = ZooSchema.locateClass(pm, cName2);
		assertEquals(stt, s1.getSuperClass());
		assertEquals(s1, s2.getSuperClass());
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}

	
	@Test
	public void testRemoveClassRollback() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass s1 = ZooSchema.declareClass(pm, "MyClass");
		s1.remove();
		
		ZooClass s2 = ZooSchema.locateClass(pm, "MyClass");
		assertNull(s2);
		
		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();
		
		ZooClass s3 = ZooSchema.locateClass(pm, "MyClass");
		assertNull(s3);
		ZooClass s4 = ZooSchema.declareClass(pm, "MyClass");
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		ZooClass s5 = ZooSchema.locateClass(pm, "MyClass");
		assertNotNull(s5);
		s5.remove();

		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();
		
		ZooClass s6 = ZooSchema.locateClass(pm, "MyClass");
		assertNotNull(s6);
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}

	
	public void testRemoveClassCommit() {
		PersistenceManager pm = TestTools.openPM();
		
		//delete uncommitted
		pm.currentTransaction().begin();
		ZooClass s1 = ZooSchema.declareClass(pm, "MyClass");
		s1.remove();

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		//delete committed
		ZooClass s3 = ZooSchema.locateClass(pm, "MyClass");
		assertNull(s3);
		ZooClass s4 = ZooSchema.declareClass(pm, "MyClass");
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		ZooClass s5 = ZooSchema.locateClass(pm, "MyClass");
		s5.remove();
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		ZooClass s6 = ZooSchema.locateClass(pm, "MyClass");
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
		ZooClass s1 = ZooSchema.declareClass(pm, "MyClass");
		s1.remove();
		try {
			s1.remove();
			fail();
		} catch (JDOUserException e) {
			//good
		}

		//remove committed
		ZooClass s2 = ZooSchema.declareClass(pm, "MyClass2");
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		s2 = ZooSchema.locateClass(pm, "MyClass2");
		s2.remove();
		try {
			s2.remove();
			fail();
		} catch (JDOUserException e) {
			//good
		}

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		s2 = ZooSchema.locateClass(pm, "MyClass2");
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
		ZooClass stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		ZooClass s1 = ZooSchema.declareClass(pm, cName1, stt);
		ZooClass s2 = ZooSchema.declareClass(pm, cName2, s1);
		s1.remove();
		assertNull(ZooSchema.declareClass(pm, cName1));
		assertNull(ZooSchema.declareClass(pm, cName2));
		
		pm.currentTransaction().rollback();
		
		//try again, this time with commit
		pm.currentTransaction().begin();
		stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		s1 = ZooSchema.declareClass(pm, cName1, stt);
		s2 = ZooSchema.declareClass(pm, cName2, s1);
		pm.currentTransaction().commit();
		TestTools.closePM();

		//load and check again
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		stt.remove();
		s1 = ZooSchema.locateClass(pm, cName1);
		s2 = ZooSchema.locateClass(pm, cName2);
		assertNull(s1);
		assertNull(s2);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		s1 = ZooSchema.locateClass(pm, cName1);
		s2 = ZooSchema.locateClass(pm, cName2);
		assertNull(stt);
		assertNull(s1);
		assertNull(s2);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}

	
	@Test
	public void testAddAttribute() {
		TestTools.defineSchema(TestClassTiny.class);
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		ZooClass s1 = ZooSchema.declareClass(pm, cName1, stt);
		ZooClass s2 = ZooSchema.declareClass(pm, cName2, s1);
		assertTrue(s1.getFields().length == 0);
		assertTrue(s2.getFields().length == 0);
		
		s1.declareField("_int", Integer.TYPE);
		s1.declareField("_long", Long.TYPE);
		s2.declareField("ref1", s1, 0);
		//TODO
		//s2.declareField("ref1Array", s1, asArray);
		
		ZooField[] fields = s1.getFields();
		if (fields[0].getFieldName().equals("_int")) {
			assertTrue(fields[1].getFieldName() == "_long");
		} else if (fields[1].getFieldName().equals("_int")) {
			assertTrue(fields[0].getFieldName() == "_long");
		} else {
			fail();
		}
		assertEquals(2, fields.length);
		
		fields = s2.getFields();
		assertEquals("ref1", fields[0].getFieldName());
		
		pm.currentTransaction().commit();
		
		//try again
		pm.currentTransaction().begin();
		stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		s1 = ZooSchema.locateClass(pm, cName1);
		s2 = ZooSchema.locateClass(pm, cName2);
		
		//check 1st class
		fields = s1.getFields();
		if (fields[0].getFieldName().equals("_int")) {
			assertTrue(fields[1].getFieldName() == "_long");
		} else if (fields[1].getFieldName().equals("_int")) {
			assertTrue(fields[0].getFieldName() == "_long");
		} else {
			fail();
		}
		assertEquals(2, fields.length);
		//check 2nd class
		fields = s2.getFields();
		assertEquals("ref1", fields[0].getFieldName());

		pm.currentTransaction().commit();
		TestTools.closePM();
//
//		//load and check again
//		pm = TestTools.openPM();
//		pm.currentTransaction().begin();
//		stt = ZooSchema.locateClass(pm, TestClassTiny.class);
//		s1 = ZooSchema.locateClass(pm, cName1);
//		s2 = ZooSchema.locateClass(pm, cName2);
//		assertEquals(stt, s1.getSuperClass());
//		assertEquals(s1, s2.getSuperClass());
//		pm.currentTransaction().rollback();
	}

	@Test
	public void testRemoveAttribute() {
		TestTools.defineSchema(TestClassTiny.class);
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		ZooClass s1 = ZooSchema.declareClass(pm, cName1, stt);
		ZooClass s2 = ZooSchema.declareClass(pm, cName2, s1);
		assertTrue(s1.getFields().length == 0);
		assertTrue(s2.getFields().length == 0);
		
		ZooField f11 = s1.declareField("_int", Integer.TYPE);
		ZooField f12 = s1.declareField("_long", Long.TYPE);
		ZooField f21 = s2.declareField("ref1", s1, 0);
		ZooField f22 = s2.declareField("ref1Array", s1, 1);

		f11.remove();
		f21.remove();
		
		ZooField[] fields1 = s1.getFields();
		assertTrue(fields1[0].getFieldName() == "_long");
		assertEquals(1, fields1.length);
		
		ZooField[] fields2 = s1.getFields();
		assertTrue(fields2[0].getFieldName() == "ref1Array");
		assertEquals(1, fields2.length);

//		fields = s2.getFields();
//		assertEquals("ref1", fields[0].getFieldName());
//		
//		pm.currentTransaction().commit();
//		
//		//try again
//		pm.currentTransaction().begin();
//		stt = ZooSchema.locateClass(pm, TestClassTiny.class);
//		s1 = ZooSchema.locateClass(pm, cName1);
//		s2 = ZooSchema.locateClass(pm, cName2);
//		
//		//check 1st class
//		fields = s1.getFields();
//		if (fields[0].getFieldName().equals("_int")) {
//			assertTrue(fields[1].getFieldName() == "_long");
//		} else if (fields[1].getFieldName().equals("_int")) {
//			assertTrue(fields[0].getFieldName() == "_long");
//		} else {
//			fail();
//		}
//		assertEquals(2, fields.length);
//		//check 2nd class
//		fields = s2.getFields();
//		assertEquals("ref1", fields[0].getFieldName());
//
//		pm.currentTransaction().commit();
//		TestTools.closePM();
////
////		//load and check again
////		pm = TestTools.openPM();
////		pm.currentTransaction().begin();
////		stt = ZooSchema.locateClass(pm, TestClassTiny.class);
////		s1 = ZooSchema.locateClass(pm, cName1);
////		s2 = ZooSchema.locateClass(pm, cName2);
////		assertEquals(stt, s1.getSuperClass());
////		assertEquals(s1, s2.getSuperClass());
////		pm.currentTransaction().rollback();
	}

}

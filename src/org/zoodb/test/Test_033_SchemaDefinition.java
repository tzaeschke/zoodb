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

	
	//TODO @Test
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
		//TODO
		//TODO
		//TODO
		//TODO
		//TODO
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		s1 = ZooSchema.locateClass(pm, cName1);
		s2 = ZooSchema.locateClass(pm, cName2);
		assertEquals(stt, s1.getSuperClass());
		assertEquals(s1, s2.getSuperClass());
		pm.currentTransaction().rollback();
	}

	
	//TODO @Test
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
		s2.declareField("ref1", s1);
		//TODO
		//s2.declareField("ref1Array", s1, asArray);
		
		pm.currentTransaction().rollback();
//		
//		//try again
//		pm.currentTransaction().begin();
//		stt = ZooSchema.locateClass(pm, TestClassTiny.class);
//		s1 = ZooSchema.declareClass(pm, cName1, stt);
//		s2 = ZooSchema.declareClass(pm, cName2, s1);
//		assertEquals(stt, s1.getSuperClass());
//		assertEquals(s1, s2.getSuperClass());
//		pm.currentTransaction().commit();
//		TestTools.closePM();
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

}

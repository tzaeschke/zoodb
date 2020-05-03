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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;

import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.internal.util.Util;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.schema.ZooClass;
import org.zoodb.schema.ZooHandle;
import org.zoodb.test.testutil.TestTools;

public class Test_036_SchemaInstanceHandling {
	
	private PersistenceManager pm;
	
	@AfterClass
	public static void tearDown() {
	    TestTools.closePM();
	}

	@Before
	public void before() {
		TestTools.createDb();
		TestTools.closePM();
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooJdoHelper.schema(pm).addClass(TestClassTiny.class);
		ZooJdoHelper.schema(pm).addClass(TestClassTiny2.class);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
	}
	
	@After
	public void after() {
		if (pm != null && !pm.isClosed() && pm.currentTransaction().isActive()) {
			pm.currentTransaction().rollback();
		}
		TestTools.closePM();
		pm = null;
		TestTools.removeDb();
	}

	
	@Test
	public void testNewInstanceWithOidFail1() {
		ZooClass c1 = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		ZooHandle hdl1 = c1.newInstance();
		try {
			c1.newInstance(hdl1.getOid());
			fail();
		} catch (IllegalArgumentException e) {
			//good
		}
	}
	
	@Test
	public void testNewInstanceWithOidFail2() {
		ZooClass c1 = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		TestClassTiny t = new TestClassTiny();
		pm.makePersistent(t);
		try {
			c1.newInstance((Long)pm.getObjectId(t));
			fail();
		} catch (IllegalArgumentException e) {
			//good
		}
	}
	
	@Test
	public void testNewInstance2PC() {
		ZooClass c1 = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		ZooHandle hdl1 = c1.newInstance();
		try {
			//converting a new Handle to an object is not allowed/supported
			hdl1.getJavaObject();
			fail();
		} catch (UnsupportedOperationException e) {
			//good
		}
	}
	
	@Test
	public void testUniqueHandle() {
		ZooClass c1 = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		ZooHandle hdl1 = c1.newInstance();
		
		ZooHandle hdl2 = ZooJdoHelper.schema(pm).getHandle(hdl1.getOid());
		assertTrue(hdl1 == hdl2);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		ZooHandle hdl3 = ZooJdoHelper.schema(pm).getHandle(hdl1.getOid());
		assertTrue(hdl1 == hdl3);
		
		Iterator<ZooHandle> hIt = 
				ZooJdoHelper.schema(pm).getClass(TestClassTiny.class).getHandleIterator(false);
		ZooHandle hdl4 = hIt.next();
		assertEquals(hdl1.getOid(), hdl4.getOid());
		assertTrue(hdl4 == hdl1);
	}
	
	@Test
	public void testPc2HandleWithNew() {
		final int I = 123;
		TestClassTiny t1 = new TestClassTiny();
		t1.setInt(I);
		pm.makePersistent(t1);
		long oid1 = (Long) pm.getObjectId(t1);
		try {
			//handles on new/dirty Java objects are not supported
			ZooJdoHelper.schema(pm).getHandle(oid1);
			fail();
		} catch (UnsupportedOperationException e) {
			//good
		}
//		assertEquals(t1.getInt(), hdl.getValue("_int"));
//		assertTrue(t1 == hdl.getJavaObject());
	}
	
	@Test
	public void testPc2Handle() {
		final int I = 123; //to avoid activation of t1
		TestClassTiny t1 = new TestClassTiny();
		t1.setInt(I);
		pm.makePersistent(t1);
		long oid1 = (Long) pm.getObjectId(t1);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		ZooHandle hdl = ZooJdoHelper.schema(pm).getHandle(oid1);
		assertEquals(I, hdl.getValue("_int"));  //'I' avoids activation of t1
		assertTrue(t1 == hdl.getJavaObject());
	}
	
	@Test
	public void testGo2Pc() {
		final int I = 123; //to avoid activation of t1
		TestClassTiny t1 = new TestClassTiny();
		t1.setInt(I);
		pm.makePersistent(t1);
		long oid1 = (Long) pm.getObjectId(t1);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooHandle hdl = ZooJdoHelper.schema(pm).getHandle(oid1);
		assertEquals(I, hdl.getValue("_int"));  //'I' avoids activation of t1
		//no load the PCI
		t1 = (TestClassTiny) pm.getObjectById(oid1);
		assertEquals(I, t1.getInt());  //activation of t1
		
		//check identity
		assertNotNull(t1);
		assertTrue(t1 == hdl.getJavaObject());
	}
	
	/**
	 * Verify that commit fails if both the PC and the GO are dirty-new.
	 */
	@Test
	public void testDoubleDirtyNewFail() {
		TestClassTiny t1 = new TestClassTiny();
		pm.makePersistent(t1);
		long oid1 = (Long) pm.getObjectId(t1);
		try {
			//handles on new/dirty Java objects are not supported
			ZooJdoHelper.schema(pm).getHandle(oid1);
			fail();
		} catch (UnsupportedOperationException e) {
			//good
		}
//		ZooHandle hdl = ZooSchema.getHandle(pm, oid1);
//		hdl.setValue("_int", 3);
//		
//		try {
//			pm.currentTransaction().commit();
//			fail();
//		} catch (JDOUserException e) {
//			//good
//		}
	}
	
	/**
	 * Verify that commit fails if both the PC and the GO are dirty.
	 */
	@Test
	public void testDoubleDirtyFail() {
		TestClassTiny t1 = new TestClassTiny();
		pm.makePersistent(t1);
		long oid1 = (Long) pm.getObjectId(t1);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		ZooHandle hdl = ZooJdoHelper.schema(pm).getHandle(oid1);
		hdl.setValue("_int", 3);
		t1.setLong(5);
		
		try {
			pm.currentTransaction().commit();
			fail();
		} catch (JDOUserException e) {
			//good
		}
	}
	
	@Test
	public void testGetJavaObjectFailForClassName() {
		TestClassTiny t1 = new TestClassTiny();
		pm.makePersistent(t1);
		long oid1 = (Long) pm.getObjectId(t1);

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//rename class
		ZooJdoHelper.schema(pm).getClass(TestClassTiny.class).rename("x.y.z");
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooHandle hdl = ZooJdoHelper.schema(pm).getHandle(oid1);
		try {
			hdl.getJavaObject();
			fail();
		} catch (JDOUserException e) {
			//good, there is no class x.y.z
		}
	}
	
	@Test
	public void testGetJavaObjectFailForFieldName() {
		TestClassTiny t1 = new TestClassTiny();
		pm.makePersistent(t1);
		long oid1 = (Long) pm.getObjectId(t1);

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//rename class
		ZooJdoHelper.schema(pm).getClass(TestClassTiny.class).getField("_int").rename("_int2");
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooHandle hdl = ZooJdoHelper.schema(pm).getHandle(oid1);
		try {
			hdl.getJavaObject();
			fail();
		} catch (JDOUserException e) {
			//good, there is no field _int2
		}
	}
	
	@Test
	public void testHandleToString() {
		ZooClass c1 = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		ZooHandle hdl1 = c1.newInstance();
		
		String oStr = Util.oidToString(hdl1.getOid());
		assertEquals(oStr, hdl1.toString());
	}
	
	/**
	 * This is relevant for JDOOptimisticVerificationExceptions. We return the Handle in the
	 * Exception, but for convenience, JDOHelper and PM should return the proper OID if required.
	 */
	@Test
	public void testHandleWithJdoObjectID() {
		ZooClass c1 = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		ZooHandle hdl1 = c1.newInstance();
		
		long oid = hdl1.getOid();
		assertEquals(oid, JDOHelper.getObjectId(hdl1));
		assertEquals(oid, pm.getObjectId(hdl1));
		
		try {
			//converting a new Handle to an object is not allowed/supported
			hdl1.getJavaObject();
			fail();
		} catch (UnsupportedOperationException e) {
			//good
		}
	}
	

}

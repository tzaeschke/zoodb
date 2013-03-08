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
package org.zoodb.profiler.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;

import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.api.ZooHandle;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.profiler.test.data.JB0;
import org.zoodb.profiler.test.data.JB1;
import org.zoodb.profiler.test.data.JB2;
import org.zoodb.profiler.test.data.JB3;
import org.zoodb.profiler.test.data.JB4;
import org.zoodb.profiler.test.data.JdoIndexedPilot;
import org.zoodb.profiler.test.data.JdoPilot;
import org.zoodb.profiler.test.util.TestTools;
import org.zoodb.test.TestClass;

public class Test_031_SchemaReading {

	@BeforeClass
	public static void setUp() {
		TestTools.createDb();
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooSchema.defineClass(pm, TestClass.class);
		ZooSchema.defineClass(pm, JdoPilot.class);
		ZooSchema.defineClass(pm, JB0.class);
		ZooSchema.defineClass(pm, JB1.class);
		ZooSchema.defineClass(pm, JB2.class);
		ZooSchema.defineClass(pm, JB3.class);
		ZooSchema.defineClass(pm, JB4.class);
		ZooSchema.defineClass(pm, JdoIndexedPilot.class);
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
	@AfterClass
	public static void tearDown() {
	    TestTools.closePM();
		TestTools.removeDb();
	}

	@After
	public void after() {
		TestTools.closePM();
	}
	
	@Test
	public void testSchemaZooHandle() {
		PersistenceManager pm0 = TestTools.openPM();
		pm0.currentTransaction().begin();
		
		TestClass t1 = new TestClass();
		t1.setData(1234567, true, 'x', (byte)126, (short)32000, 12345678901L, "haha", 
				new byte[]{1, 2, 3}, -1.1f, 33.3);
		TestClass t2 = new TestClass();
		t2.setData(-1234567, true, 'y', (byte)-126, (short)-32000, -12345678901L, "hihi", 
				new byte[]{-1, -2, -3}, 11.11f, -3.3);
		
		pm0.makePersistent(t1);
		pm0.makePersistent(t2);
		
		long oid1 = (Long) pm0.getObjectId(t1);
		long oid2 = (Long) pm0.getObjectId(t2);
		
		pm0.currentTransaction().commit();
		TestTools.closePM();
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooClass s01 = ZooSchema.locateClass(pm, TestClass.class.getName());
		assertNotNull(s01);

		//closed pm
		try {
			ZooSchema.getHandle(pm0, oid1);
			fail();
		} catch (JDOObjectNotFoundException e) {
			//good!
		}
		
		//wrong oid
		try {
			ZooSchema.getHandle(pm0, 12345678);
			fail();
		} catch (JDOObjectNotFoundException e) {
			//good!
		}
		
		ZooHandle hdl1 = ZooSchema.getHandle(pm, oid1);
		ZooHandle hdl2 = ZooSchema.getHandle(pm, oid2);
		assertNotNull(hdl1);
		assertNotNull(hdl2);
		
		assertEquals((Long)oid1, (Long)hdl1.getOid());
		assertEquals(s01, hdl1.getType());
		
		TestTools.closePM();
	}
	

	@Test
	public void testSchemaZooHandleReader() {
		PersistenceManager pm0 = TestTools.openPM();
		pm0.currentTransaction().begin();
		
		TestClass t1 = new TestClass();
		t1.setData(1234567, true, 'x', (byte)126, (short)32000, 12345678901L, "haha", 
				new byte[]{1, 2, 3}, -1.1f, 33.3);
		TestClass t2 = new TestClass();
		t2.setData(-1234567, true, 'y', (byte)-126, (short)-32000, -12345678901L, "hihi", 
				new byte[]{-1, -2, -3}, 11.11f, -3.3);
		
		t1.setRef2(t2);
		
		pm0.makePersistent(t1);
		pm0.makePersistent(t2);
		
		long oid1 = (Long) pm0.getObjectId(t1);
		long oid2 = (Long) pm0.getObjectId(t2);
		
		pm0.currentTransaction().commit();
		TestTools.closePM();
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooClass s01 = ZooSchema.locateClass(pm, TestClass.class.getName());
		assertNotNull(s01);

		ZooHandle hdl1 = ZooSchema.getHandle(pm, oid1);
		ZooHandle hdl2 = ZooSchema.getHandle(pm, oid2);

		//TODO
//		TestTools.closePM();
//		fail();
		
		assertEquals(126, hdl1.getAttrByte("_byte"));
		assertEquals(1234567, hdl1.getAttrInt("_int"));
		assertEquals(true, hdl1.getAttrBool("_bool"));
		assertEquals('x', hdl1.getAttrChar("_char"));
		assertEquals(32000, hdl1.getAttrShort("_short"));
		assertEquals(12345678901L, hdl1.getAttrLong("_long"));
		assertEquals(-1.1f, hdl1.getAttrFloat("_float"), 0.0);
		assertEquals(33.3, hdl1.getAttrDouble("_double"), 0.0);
//TODO		assertEquals("haha", hdl1.getAttrString("_string"));
		
		long oid2b = hdl1.getAttrRefOid("_ref2");
		assertEquals(oid2, oid2b);
		
		
		TestTools.closePM();
	}
	

	@Test
	public void testSchemaZooHandleReaderOnRenamed() {
		PersistenceManager pm0 = TestTools.openPM();
		pm0.currentTransaction().begin();
		
		TestClass t1 = new TestClass();
		t1.setData(1234567, true, 'x', (byte)126, (short)32000, 12345678901L, "haha", 
				new byte[]{1, 2, 3}, -1.1f, 33.3);
		TestClass t2 = new TestClass();
		t2.setData(-1234567, true, 'y', (byte)-126, (short)-32000, -12345678901L, "hihi", 
				new byte[]{-1, -2, -3}, 11.11f, -3.3);
		
		t1.setRef2(t2);
		
		pm0.makePersistent(t1);
		pm0.makePersistent(t2);
		
		long oid1 = (Long) pm0.getObjectId(t1);
		long oid2 = (Long) pm0.getObjectId(t2);
		
		pm0.currentTransaction().commit();
		TestTools.closePM();
		
		//rename schema
		pm0 = TestTools.openPM();
		pm0.currentTransaction().begin();
		ZooClass s = ZooSchema.locateClass(pm0, TestClass.class);
		s.rename("x");
		pm0.currentTransaction().commit();
		TestTools.closePM();

		
		//test renamed
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooClass s01 = ZooSchema.locateClass(pm, "x");
		assertNotNull(s01);

		ZooHandle hdl1 = ZooSchema.getHandle(pm, oid1);
		ZooHandle hdl2 = ZooSchema.getHandle(pm, oid2);

		//TODO
//		TestTools.closePM();
//		fail();
		
		assertEquals(126, hdl1.getAttrByte("_byte"));
		assertEquals(1234567, hdl1.getAttrInt("_int"));
		assertEquals(true, hdl1.getAttrBool("_bool"));
		assertEquals('x', hdl1.getAttrChar("_char"));
		assertEquals(32000, hdl1.getAttrShort("_short"));
		assertEquals(12345678901L, hdl1.getAttrLong("_long"));
		assertEquals(-1.1f, hdl1.getAttrFloat("_float"), 0.0);
		assertEquals(33.3, hdl1.getAttrDouble("_double"), 0.0);
//TODO		assertEquals("haha", hdl1.getAttrString("_string"));
		
		long oid2b = hdl1.getAttrRefOid("_ref2");
		assertEquals(oid2, oid2b);
		
		TestTools.closePM();
		
		//rename back
		pm0 = TestTools.openPM();
		pm0.currentTransaction().begin();
		s = ZooSchema.locateClass(pm0, "x");
		s.rename(TestClass.class.getName());
		pm0.currentTransaction().commit();
		TestTools.closePM();
	}
	

	//Check that the handle attribute access works across page boundaries.
	@Test
	public void testSchemaZooHandleReaderWithPageWrap() {
		//populate
		PersistenceManager pm0 = TestTools.openPM();
		pm0.currentTransaction().begin();
		ArrayList<Long> oids = new ArrayList<Long>();
		for (int i = 0; i < 10000; i++) {
			TestClass t1 = new TestClass();
			t1.setData(1234567, true, 'x', (byte)126, (short)32000, 12345678901L, "haha", 
					new byte[]{1, 2, 3}, -1.1f, 33.3);
			t1.setRef2(t1);
			pm0.makePersistent(t1);
			oids.add( (Long)pm0.getObjectId(t1) );
		}
		pm0.currentTransaction().commit();
		TestTools.closePM();
		
		//check
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		for (Long oid: oids) {
			ZooHandle hdl1 = ZooSchema.getHandle(pm, oid);
			assertEquals(126, hdl1.getAttrByte("_byte"));
			assertEquals(1234567, hdl1.getAttrInt("_int"));
			assertEquals(true, hdl1.getAttrBool("_bool"));
			assertEquals('x', hdl1.getAttrChar("_char"));
			assertEquals(32000, hdl1.getAttrShort("_short"));
			assertEquals(12345678901L, hdl1.getAttrLong("_long"));
			assertEquals(-1.1f, hdl1.getAttrFloat("_float"), 0.0);
			assertEquals(33.3, hdl1.getAttrDouble("_double"), 0.0);
			//TODO		assertEquals("haha", hdl1.getAttrString("_string"));
			long oid2 = hdl1.getAttrRefOid("_ref2");
			assertEquals((long)oid, oid2);
		}
		
		TestTools.closePM();
	}
	

	@Test
	public void testSchemaDeletion() {
		System.out.println("Testing Schema deletion - TODO"); //TODO
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		//ensure schema not in DB, only in cache
		ZooClass s01 = ZooSchema.locateClass(pm, TestClass.class.getName());
		assertNotNull(s01);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
		
	
	@Test
	public void testSchemaHierarchy() {
		//test that allocating 6 schemas does not require too many pages //TODO?
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		assertNotNull( ZooSchema.locateClass(pm, JB0.class) );
		assertNotNull( ZooSchema.locateClass(pm, JB1.class) );
		assertNotNull( ZooSchema.locateClass(pm, JB2.class) );
		assertNotNull( ZooSchema.locateClass(pm, JB3.class) );
		assertNotNull( ZooSchema.locateClass(pm, JB4.class) );
		
		JB4 jb4 = new JB4();
		pm.makePersistent(jb4);
		JB0 jb0 = new JB0();
		pm.makePersistent(jb0);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
}

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
import static org.junit.Assert.fail;

import java.util.Iterator;

import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.api.ZooField;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.jdo.internal.ZooHandle;
import org.zoodb.test.util.TestTools;

public class Test_034_SchemaEvolution {
	
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
	    System.out.println("TEST AFTER 34");
		try {
			TestTools.closePM();
		} catch (Throwable t) {
			t.printStackTrace();
		}
		TestTools.removeDb();
	}

	
	@Test
	public void testSimpleEvolution() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooSchema.defineClass(pm, TestClassTiny.class);
		TestClassTiny t1 = new TestClassTiny(1, 3);
		TestClassTiny t2 = new TestClassTiny(4, 5);
		pm.makePersistent(t1);
		pm.makePersistent(t2);
		Object oid1 = pm.getObjectId(t1);
		Object oid2 = pm.getObjectId(t2);
		pm.currentTransaction().commit();
		
		TestTools.closePM();
		pm = TestTools.openPM();
		
		pm.currentTransaction().begin();
		
		ZooClass s1 = ZooSchema.locateClass(pm, TestClassTiny.class.getName());
		s1.rename(TestClassSmall.class.getName());
//		private int myInt;
//		private long myLong;
//		private String myString;
//		private int[] myInts;
//		private Object refO;
//		private TestClassTiny refP;

		s1.locateField("_int").remove();
		s1.declareField("myInt", Integer.TYPE);
		s1.locateField("_long").rename("myLong");
		s1.declareField("myString", String.class);
		s1.declareField("myInts", Integer[].class);
		s1.declareField("refO", Object.class);
		s1.declareField("refP", TestClassTiny.class);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClassSmall ts1 = (TestClassSmall) pm.getObjectById(oid1);
		TestClassSmall ts2 = (TestClassSmall) pm.getObjectById(oid2);

		assertEquals(0, ts1.getMyInt());
		assertEquals(3, ts1.getMyLong());
		assertEquals(0, ts2.getMyInt());
		assertEquals(5, ts2.getMyLong());
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}
	
	@Test
	public void testDataMigration() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooSchema.defineClass(pm, TestClassTiny.class);
		TestClassTiny t1 = new TestClassTiny(1, 3);
		TestClassTiny t2 = new TestClassTiny(4, 5);
		pm.makePersistent(t1);
		pm.makePersistent(t2);
		Object oid1 = pm.getObjectId(t1);
		Object oid2 = pm.getObjectId(t2);
		pm.currentTransaction().commit();
		
		TestTools.closePM();
		pm = TestTools.openPM();
		
		pm.currentTransaction().begin();
		
		ZooClass s1 = ZooSchema.locateClass(pm, TestClassTiny.class.getName());
		s1.rename(TestClassSmall.class.getName());
//		private int myInt;
//		private long myLong;
//		private String myString;
//		private int[] myInts;
//		private Object refO;
//		private TestClassTiny refP;

		//additive changes
		s1.declareField("myInt", Integer.TYPE);
		s1.locateField("_long").rename("myLong");
		s1.declareField("myString", String.class);
		s1.declareField("myInts", Integer[].class);
		s1.declareField("refO", Object.class);
		s1.declareField("refP", TestClassTiny.class);
		
		Iterator<ZooHandle> it = s1.getHandleIterator(false);
		int n = 0;
		while (it.hasNext()) {
			//migrate data
			ZooField f1 = s1.locateField("_int");
			ZooField f2a = s1.locateField("myInt"); //new
			ZooField f2b = s1.locateField("myLong");  //renamed
			ZooField f2c = s1.locateField("myString");  //new String
			
			ZooHandle hdl = it.next();
			//TODO pass in field instead?!?!
			int i = (Integer) f1.getValue(hdl);
			f2a.setValue(hdl, i+1);
			f2b.setValue(hdl, (long)i+2);
			f2c.setValue(hdl, String.valueOf(i+3));
			
	        assertEquals(i+1, (int)(Integer) f2a.getValue(hdl));
	        assertEquals(i+2, (long)(Long) f2b.getValue(hdl));
	        assertEquals(String.valueOf(i+=3), f2c.getValue(hdl));
	        
			//batch processing 
			pm.currentTransaction().commit();
			pm.currentTransaction().begin();
			n++;
		}
		assertEquals(2, n);
		//destructive changes
		s1.locateField("_int").remove();
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClassSmall ts1 = (TestClassSmall) pm.getObjectById(oid1);
		TestClassSmall ts2 = (TestClassSmall) pm.getObjectById(oid2);

		assertEquals(2, ts1.getMyInt());
        assertEquals(3, ts1.getMyLong());
        assertEquals("4", ts1.getMyString());
		assertEquals(5, ts2.getMyInt());
		assertEquals(6, ts2.getMyLong());
        assertEquals("7", ts2.getMyString());
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}
	
    @Test
    public void testSetOid() {
        fail();
    }

    /**
     * Check that non-evolved objects are still returned when a query matches the default value
     * of a recently added field.
     */
    @Test
    public void testQueryNonEvolvedObjectsOnDefaultValueWithFieldIndex() {
        fail();
    }

    @Test
    public void testCleanUpOfPreviousVersions() {
        //query on class oid?
        //db size?
        //iterate old pos-index?
        //iterate value-index?
        fail();
    }

    /**
     * Test that an object that is evolved becomes rewritten. How can we check this?
     * a) Without modification, the database should still write pages (change size?)
     * b) The object should be dirty? (Actually, not necessarily, the user should not notice)
     * c) Instance callbacks? The same as b)
     * 
     * --> Enable/disable isEvolved in DataDeSerializer 
     */
    @Test
    public void testEvolutionReWrite() {
        fail();
    }

}

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

import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.api.ZooSchema;
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
		assertEquals(5, ts1.getMyLong());
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}

}

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.api.ZooHandle;
import org.zoodb.jdo.api.ZooSchema;
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
		ZooSchema.defineClass(pm, TestClassTiny.class);
		ZooSchema.defineClass(pm, TestClassTiny2.class);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
	}
	
	@After
	public void after() {
		pm.currentTransaction().rollback();
		TestTools.closePM();
		pm = null;
		TestTools.removeDb();
	}

	
	@Test
	public void testNewInstance() {
		ZooClass c1 = ZooSchema.locateClass(pm, TestClassTiny.class);
		ZooHandle hdl1 = c1.newInstance();
		try {
			c1.newInstance((Long)hdl1.getOid());
		} catch (IllegalArgumentException e) {
			//good
		}
	}
	
	@Test
	public void testUniqueHandle() {
		ZooClass c1 = ZooSchema.locateClass(pm, TestClassTiny.class);
		ZooHandle hdl1 = c1.newInstance();
		
		ZooHandle hdl2 = ZooSchema.getHandle(pm, hdl1.getOid());
		assertTrue(hdl1 == hdl2);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		ZooHandle hdl3 = ZooSchema.getHandle(pm, hdl1.getOid());
		assertTrue(hdl1 == hdl3);
		
		Iterator<ZooHandle> hIt = 
				ZooSchema.locateClass(pm, TestClassTiny.class).getHandleIterator(false);
		ZooHandle hdl4 = hIt.next();
		assertEquals(hdl1.getOid(), hdl4.getOid());
		assertTrue(hdl4 == hdl1);
	}
}

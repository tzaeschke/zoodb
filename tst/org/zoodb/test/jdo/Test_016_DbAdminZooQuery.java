/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
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

import static org.junit.Assert.assertTrue;

import javax.jdo.PersistenceManager;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.util.Util;
import org.zoodb.test.testutil.TestTools;
import org.zoodb.tools.ZooQuery;

public class Test_016_DbAdminZooQuery {
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb();
		ZooQuery.enableStringOutput();
		TestTools.defineSchema(TestClass.class);
	}
	
	@Before
	public void before() {
		ZooQuery.resetStringOutput();
	}

	@Test
	public void testQueryEmpty() {
		ZooQuery.main(new String[]{TestTools.getDbName(), "select", "from", ZooPC.class.getName()});
		String out = ZooQuery.getStringOutput();
		assertTrue(out.contains("Querying database done."));
	}
	
	@Test
	public void testQuery() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClass t = new TestClass();
		t.setString("Hello");
		pm.makePersistent(t);
		Object oid = pm.getObjectId(t);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		
		ZooQuery.main(new String[]{TestTools.getDbName(), "select", "from", ZooPC.class.getName()});
		String out = ZooQuery.getStringOutput();
		//System.out.println("out=" + out);
		assertTrue(out.contains(Util.oidToString(oid)));
		assertTrue(out.contains("Querying database done."));
	}
	
	@Test
	public void testQueryEasy() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClass t = new TestClass();
		t.setString("HelloEasy");
		pm.makePersistent(t);
		Object oid = pm.getObjectId(t);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		
		ZooQuery.main(new String[]{TestTools.getDbName(), "select", "from", "TestClass"});
		String out = ZooQuery.getStringOutput();
		//System.out.println("out=" + out);
		assertTrue(out.contains(Util.oidToString(oid)));
		assertTrue(out.contains("Querying database done."));
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
	}
}

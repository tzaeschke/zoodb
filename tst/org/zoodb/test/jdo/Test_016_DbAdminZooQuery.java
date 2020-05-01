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

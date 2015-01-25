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

import static org.junit.Assert.assertEquals;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.api.DBArrayList;
import org.zoodb.api.DBHashMap;
import org.zoodb.test.testutil.TestTools;

public class Test_083_SerailizationDbCollections {


	@Before
	public void before() {
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
	}

	/**
	 * Run after each test.
	 */
	@After
	public void after() {
		TestTools.closePM();
	}


	/**
	 * This used to fail because deserialized DbCollection were marked dirty.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testSerialization() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		DBHashMap<TestClass, TestClass> h1 = new DBHashMap<TestClass, TestClass>();
		DBHashMap<TestClass, TestClass> h2 = new DBHashMap<TestClass, TestClass>();
		TestClass k = new TestClass();
		h2.put(k, new TestClass());
		DBArrayList<TestClass> l1 = new DBArrayList<TestClass>();
		DBArrayList<TestClass> l2 = new DBArrayList<TestClass>();
		l2.add(new TestClass());
		pm.makePersistent(h1);
		pm.makePersistent(h2);
		pm.makePersistent(k);
		pm.makePersistent(l1);
		pm.makePersistent(l2);
		Object oid1 = pm.getObjectId(h1);
		Object oid2 = pm.getObjectId(h2);
		Object oidk = pm.getObjectId(k);
		Object oid3 = pm.getObjectId(l1);
		Object oid4 = pm.getObjectId(l2);
		pm.currentTransaction().commit();
		TestTools.closePM();

		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		h1 = (DBHashMap<TestClass, TestClass>) pm.getObjectById(oid1);
		assertEquals(false, JDOHelper.isDirty(h1));
		h2 = (DBHashMap<TestClass, TestClass>) pm.getObjectById(oid2);
		assertEquals(false, JDOHelper.isDirty(h2));
		k = (TestClass) pm.getObjectById(oidk);
		assertEquals(false, JDOHelper.isDirty(k));
		assertEquals(false, JDOHelper.isDirty(h2.get(k)));
		
		l1 = (DBArrayList<TestClass>) pm.getObjectById(oid3);
		assertEquals(false, JDOHelper.isDirty(l1));
		l2 = (DBArrayList<TestClass>) pm.getObjectById(oid4);
		assertEquals(false, JDOHelper.isDirty(l2));
		assertEquals(false, JDOHelper.isDirty(l2.get(0)));
		
		pm.currentTransaction().commit();
		pm.close();
	}
}


/*
 * Copyright 2009-2013 Tilmann Zäschke. All rights reserved.
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.util.TestTools;

public class Test_047_TransactionsManagedObjects {

	@BeforeClass
	public static void beforeClass() {
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
	}

	@Before
	public void before() {
		TestTools.dropInstances(TestClass.class);
	}

	@After
	public void after() {
		TestTools.closePM();
	}
	
	@AfterClass
	public static void afterClass() {
		TestTools.removeDb();
	}
	
	@Test
	public void testGetManagedObjects() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClass t1 = new TestClass();

		assertFalse(pm.getManagedObjects().contains(t1));

		pm.makePersistent(t1);
		assertTrue(pm.getManagedObjects().contains(t1));
		assertTrue(JDOHelper.isDirty(t1));
		
		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();
		
		assertFalse(pm.getManagedObjects().contains(t1));

		pm.currentTransaction().rollback();
		TestTools.closePM();
	}

}

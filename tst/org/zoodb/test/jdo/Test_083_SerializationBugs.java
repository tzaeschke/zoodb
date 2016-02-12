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

import static org.junit.Assert.assertNotNull;

import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.api.impl.ZooPC;
import org.zoodb.test.testutil.TestTools;

public class Test_083_SerializationBugs {


	@Before
	public void before() {
		// nothing
	}


	/**
	 * Run after each test.
	 */
	@After
	public void after() {
		TestTools.closePM();
	}


	@BeforeClass
	public static void beforeClass() {
		TestTools.createDb();
	}


	@AfterClass
	public static void afterClass() {
		TestTools.removeDb();
	}


	/**
	 * Test serialisation. 
	 * 
	 * If a transaction has only new objects (no modified objects) and if the new objects have 
	 * other persistent capable objects referenced (but no have called makePersistent() on them) 
	 * and if we are not in the first transaction in a session, then we get the following NPE.
	 * See issue #57.
	 * 
	 */
	@Test
	public void testSerialization() {
		TestTools.defineSchema(ProjectPC.class, CoordinatePC.class);
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		CoordinatePC x = new CoordinatePC();
		pm.makePersistent(x);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		ProjectPC s = new ProjectPC(new ProjectPC(null));
		pm.makePersistent(s);
		Object oid = pm.getObjectId(s);
		pm.currentTransaction().commit();
		TestTools.closePM();

		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ProjectPC s2 = (ProjectPC) pm.getObjectById(oid);
		assertNotNull(s2);
		pm.currentTransaction().commit();
		pm.close();
	}

}


class ProjectPC extends ZooPC {

	@SuppressWarnings("unused")
	private ProjectPC coor;
	
	public ProjectPC() {
		// Auto-generated constructor stub
	}
	
	protected ProjectPC(ProjectPC pc) {
		coor = pc;
	}
	
}

class CoordinatePC extends ZooPC {
	
}


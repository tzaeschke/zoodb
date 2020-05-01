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


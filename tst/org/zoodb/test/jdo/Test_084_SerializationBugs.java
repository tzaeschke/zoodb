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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.api.impl.ZooPC;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.test.testutil.TestTools;

public class Test_084_SerializationBugs {


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
	 * Executing a query calls the OGT. Unfortunately, the OGT put all dirty objects on a 
	 * seenObjects list, which was never deleted. In a subsequent run (i.e. the actual commit),
	 * the object (ProjectPC) was not checked again, even if it had been modified.
	 * This usually worked because:
	 * a) updates and queries occurred in separate transactions
	 * b) updates occurred before queries
	 * c) updates often occur on primitives
	 * In all above cases, the error would not occur.
	 * See issue #58.
	 * 
	 */
	@Test
	public void testSerialization() {
		TestTools.defineSchema(ProjectPC084.class);
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		props.setZooAutoCreateSchema(true);
		PersistenceManager pm = TestTools.openPM(props);
		pm.currentTransaction().begin();

		ProjectPC084 s = new ProjectPC084();
		pm.makePersistent(s);
		
		Query q = pm.newQuery(ProjectPC084.class);
		q.execute();
		
		//System.out.println("STT=" + JDOHelper.getObjectState(s));
		s.getActiveUsers().add(new OnlineStatePC084());
		//System.out.println("STT=" + JDOHelper.getObjectState(s));
		
		Object oid = pm.getObjectId(s);
		pm.currentTransaction().commit();
		TestTools.closePM();

		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ProjectPC084 s2 = (ProjectPC084) pm.getObjectById(oid);
		assertNotNull(s2);
		pm.currentTransaction().commit();
		pm.close();
	}

}


class ProjectPC084 extends ZooPC {

	private Set<OnlineStatePC084> activeUsers;
	private Map<Long, ProjectBranchPC084> branches;

	ProjectPC084() {
		activeUsers = new HashSet<>();
		branches = new HashMap<Long, ProjectBranchPC084>();
	}

	public Map<Long, ProjectBranchPC084> getBranches() {
		this.zooActivateWrite();
		return this.branches;
	}


	public Set<OnlineStatePC084> getActiveUsers() {
		this.zooActivateWrite();
		return this.activeUsers;
	}
	
}

class OnlineStatePC084 extends ZooPC {

}

class ProjectBranchPC084 extends ZooPC {

}


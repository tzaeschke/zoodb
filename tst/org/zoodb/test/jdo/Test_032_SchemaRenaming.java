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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.schema.ZooClass;
import org.zoodb.test.testutil.TestTools;

public class Test_032_SchemaRenaming {

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
		TestTools.closePM();
		TestTools.removeDb();
	}
	
	@Test
	public void testRenameFailsIfExists() {
		TestTools.defineSchema(TestClassTiny.class);
		TestTools.defineSchema(TestClassTinyClone.class);
		
		//rename
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooClass s = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class.getName());
		
		try {
			s.rename(TestClassTinyClone.class.getName());
			fail();
		} catch (IllegalStateException e) {
			//good
		}
		TestTools.closePM();
	}
	
	@Test
	public void testSchemaRenameWithoutInstances() {
		TestTools.defineSchema(TestClassTiny.class);
		
		//rename
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooClass s = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class.getName());
		s.rename(TestClassTinyClone.class.getName());
		assertEquals(TestClassTinyClone.class.getName(), s.getName());
		//check before commit
		checkRename(TestClassTiny.class, TestClassTinyClone.class, pm);
		pm.currentTransaction().commit();
		
		//check after commit
		pm.currentTransaction().begin();
		checkRename(TestClassTiny.class, TestClassTinyClone.class, pm);
		pm.currentTransaction().commit();
		
		TestTools.closePM();
		
		//check new session
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		checkRename(TestClassTiny.class, TestClassTinyClone.class, pm);
		pm.currentTransaction().commit();
		
		TestTools.closePM();
	}
	
	@Test
	public void testSchemaRenameWithInstances() {
		TestTools.defineSchema(TestClassTiny.class);

		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		TestClassTiny pc = new TestClassTiny();
		pc.setInt(1234);
		pm.makePersistent(pc);
		Object oid = pm.getObjectId(pc);
		pm.currentTransaction().commit();
		
		TestTools.closePM();

		
		//rename
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooClass s = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class.getName());
		s.rename(TestClassTinyClone.class.getName());
		//check before commit
		checkRename(TestClassTiny.class, TestClassTinyClone.class, pm);
		Object pc2 = pm.getObjectById(oid);
		assertEquals(TestClassTinyClone.class, pc2.getClass());
		assertEquals(1234, ((TestClassTinyClone)pc2).getInt());
		pm.currentTransaction().commit();
		
		//check after commit
		pm.currentTransaction().begin();
		checkRename(TestClassTiny.class, TestClassTinyClone.class, pm);
		pc2 = pm.getObjectById(oid);
		assertEquals(TestClassTinyClone.class, pc2.getClass());
		assertEquals(1234, ((TestClassTinyClone)pc2).getInt());
		pm.currentTransaction().commit();
		
		TestTools.closePM();
		
		//check new session
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		checkRename(TestClassTiny.class, TestClassTinyClone.class, pm);
		pc2 = pm.getObjectById(oid);
		assertEquals(TestClassTinyClone.class, pc2.getClass());
		assertEquals(1234, ((TestClassTinyClone)pc2).getInt());
		pm.currentTransaction().commit();
		
		TestTools.closePM();
	}
	
	private void checkRename(Class<?> oldCls, Class<?> newCls, PersistenceManager pm) {
		assertNotNull(ZooJdoHelper.schema(pm).getClass(newCls.getName()));
		assertNull(ZooJdoHelper.schema(pm).getClass(oldCls.getName()));
		assertNotNull(ZooJdoHelper.schema(pm).getClass(newCls));
		assertNull(ZooJdoHelper.schema(pm).getClass(oldCls));
	}
		
	
	@Test
	public void testSchemaRenameWithSubClasses() {
		TestTools.defineSchema(TestClassTiny.class);
		TestTools.defineSchema(TestClassTiny2.class);

		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		TestClassTiny pc = new TestClassTiny();
		pc.setInt(1234);
		pm.makePersistent(pc);
		Object oid = pm.getObjectId(pc);
		TestClassTiny2 pc2 = new TestClassTiny2();
		pc2.setInt(1234);
		pm.makePersistent(pc2);
		Object oid2 = pm.getObjectId(pc2);
		pm.currentTransaction().commit();
		
		TestTools.closePM();

		
		//rename
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooClass s = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class.getName());
		s.rename(TestClassTinyClone.class.getName());
		ZooClass s2 = ZooJdoHelper.schema(pm).getClass(TestClassTiny2.class.getName());
		s2.rename(TestClassTinyClone2.class.getName());

		//check before commit
		checkRename(TestClassTiny.class, TestClassTinyClone.class, pm);
		checkRename(TestClassTiny2.class, TestClassTinyClone2.class, pm);
		Object obj = pm.getObjectById(oid);
		assertEquals(TestClassTinyClone.class, obj.getClass());
		assertEquals(1234, ((TestClassTinyClone)obj).getInt());
		Object obj2 = pm.getObjectById(oid2);
		assertEquals(TestClassTinyClone2.class, obj2.getClass());
		assertEquals(1234, ((TestClassTinyClone2)obj2).getInt());
		pm.currentTransaction().commit();
		
		//check after commit
		pm.currentTransaction().begin();
		checkRename(TestClassTiny.class, TestClassTinyClone.class, pm);
		checkRename(TestClassTiny2.class, TestClassTinyClone2.class, pm);
		obj = pm.getObjectById(oid);
		assertEquals(TestClassTinyClone.class, obj.getClass());
		assertEquals(1234, ((TestClassTinyClone)obj).getInt());
		pm.currentTransaction().commit();
		
		TestTools.closePM();
	}
	
	@Test
	public void testSchemaRenameRollback() {
		TestTools.defineSchema(TestClassTiny.class);
		
		//rename
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooClass s = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class.getName());
		s.rename(TestClassTinyClone.class.getName());
		//check before commit
		checkRename(TestClassTiny.class, TestClassTinyClone.class, pm);
		pm.currentTransaction().rollback();
		
		//check after commit
		pm.currentTransaction().begin();
		checkRename(TestClassTinyClone.class, TestClassTiny.class, pm);
		pm.currentTransaction().commit();
		
		TestTools.closePM();
		
		//check new session
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		checkRename(TestClassTinyClone.class, TestClassTiny.class, pm);
		pm.currentTransaction().commit();
		
		TestTools.closePM();
	}
	
	
	@Test
	public void testSchemaRenameToNonExistent() {
		TestTools.defineSchema(TestClassTiny.class);
		
		//rename
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooClass s = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class.getName());
		s.rename("x");
		assertEquals("x", s.getName());
		//check before commit
		assertNull(ZooJdoHelper.schema(pm).getClass(TestClassTiny.class.getName()));
		assertNotNull(ZooJdoHelper.schema(pm).getClass("x"));
		pm.currentTransaction().commit();
		
		//check after commit
		pm.currentTransaction().begin();
		assertNotNull(ZooJdoHelper.schema(pm).getClass("x"));
		assertNull(ZooJdoHelper.schema(pm).getClass(TestClassTiny.class.getName()));
		pm.currentTransaction().commit();
		
		TestTools.closePM();
		
		//check new session
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		assertNotNull(ZooJdoHelper.schema(pm).getClass("x"));
		assertNull(ZooJdoHelper.schema(pm).getClass(TestClassTiny.class.getName()));
		pm.currentTransaction().commit();
		
		//check rename again
		pm.currentTransaction().begin();
		ZooClass s2 = ZooJdoHelper.schema(pm).getClass("x");
		s2.rename("y");
		assertEquals("y", s2.getName());
		//check before commit
		assertNull(ZooJdoHelper.schema(pm).getClass("x"));
		assertNotNull(ZooJdoHelper.schema(pm).getClass("y"));
		pm.currentTransaction().commit();
		
		
		TestTools.closePM();
	}
	
}

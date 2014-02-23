/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.api.ZooSchema;
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
		
		ZooClass s = ZooSchema.locateClass(pm, TestClassTiny.class.getName());
		
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
		
		ZooClass s = ZooSchema.locateClass(pm, TestClassTiny.class.getName());
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
		
		ZooClass s = ZooSchema.locateClass(pm, TestClassTiny.class.getName());
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
		assertNotNull(ZooSchema.locateClass(pm, newCls.getName()));
		assertNull(ZooSchema.locateClass(pm, oldCls.getName()));
		assertNotNull(ZooSchema.locateClass(pm, newCls));
		assertNull(ZooSchema.locateClass(pm, oldCls));
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
		
		ZooClass s = ZooSchema.locateClass(pm, TestClassTiny.class.getName());
		s.rename(TestClassTinyClone.class.getName());
		ZooClass s2 = ZooSchema.locateClass(pm, TestClassTiny2.class.getName());
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
		
		ZooClass s = ZooSchema.locateClass(pm, TestClassTiny.class.getName());
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
		
		ZooClass s = ZooSchema.locateClass(pm, TestClassTiny.class.getName());
		s.rename("x");
		assertEquals("x", s.getName());
		//check before commit
		assertNull(ZooSchema.locateClass(pm, TestClassTiny.class.getName()));
		assertNotNull(ZooSchema.locateClass(pm, "x"));
		pm.currentTransaction().commit();
		
		//check after commit
		pm.currentTransaction().begin();
		assertNotNull(ZooSchema.locateClass(pm, "x"));
		assertNull(ZooSchema.locateClass(pm, TestClassTiny.class.getName()));
		pm.currentTransaction().commit();
		
		TestTools.closePM();
		
		//check new session
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		assertNotNull(ZooSchema.locateClass(pm, "x"));
		assertNull(ZooSchema.locateClass(pm, TestClassTiny.class.getName()));
		pm.currentTransaction().commit();
		
		//check rename again
		pm.currentTransaction().begin();
		ZooClass s2 = ZooSchema.locateClass(pm, "x");
		s2.rename("y");
		assertEquals("y", s2.getName());
		//check before commit
		assertNull(ZooSchema.locateClass(pm, "x"));
		assertNotNull(ZooSchema.locateClass(pm, "y"));
		pm.currentTransaction().commit();
		
		
		TestTools.closePM();
	}
	
}

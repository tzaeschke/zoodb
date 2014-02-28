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
package org.zoodb.test.jdo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Collection;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.internal.Session;
import org.zoodb.schema.ZooClass;
import org.zoodb.schema.ZooSchema;
import org.zoodb.test.testutil.TestTools;

public class Test_035_SchemaEvolutionShorts {
	
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
	public void testGetSuperClass() {
		ZooClass c1 = ZooSchema.locateClass(pm, TestClassTiny.class);
		ZooClass c0 = ZooSchema.locateClass(pm, ZooPCImpl.class);
		assertNotNull(c1.getSuperClass());
		//This caused a NPE.
		assertNull(c0.getSuperClass());
	}
	
	/**
	 * This used to fail because the internal cache mapped Java classes to schema, in the case
	 * of virtual classes it mapped 'null' to the generic class. This mapping was only used
	 * by locateAllCLasses().
	 */
	@Test
	public void testDualDeclare() {
		String n1 = "Publication";
		String n2 = "Author";
		ZooSchema.defineEmptyClass(pm, n1);
		ZooSchema.defineEmptyClass(pm, n2);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		assertNotNull(ZooSchema.locateClass(pm, n1));
		assertNotNull(ZooSchema.locateClass(pm, n2));
		
		Collection<ZooClass> cc = ZooSchema.locateAllClasses(pm);
		int hasPub = 0;
		int hasAut = 0;
		for (ZooClass c: cc) {
			if (c.getName().equals(n2)) {
				hasAut += 1;
			}
			if (c.getName().equals(n1)) {
				hasPub += 1;
			}
		}
		assertEquals(1, hasAut);
		assertEquals(1, hasPub);
	}
	
	/**
	 * This used to fail because the NoClass deserializer expected the latest class version.
	 * It also failed because c1/c2 where not using the latest schema version during commit(). 
	 */
	@Test
	public void testDropEvolvedSchema() {
		//create data
		TestClassTiny t1 = new TestClassTiny();
		TestClassTiny2 t2 = new TestClassTiny2();
		pm.makePersistent(t1);
		pm.makePersistent(t2);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		//evolve
		ZooClass c1 = ZooSchema.locateClass(pm, TestClassTiny.class.getName());
		c1.defineField("hello", Long.TYPE);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		//delete
		ZooClass c2 = ZooSchema.locateClass(pm, TestClassTiny2.class.getName());
		c2.remove();
		c1 = ZooSchema.locateClass(pm, TestClassTiny.class.getName());
		c1.remove();
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
	}
	
	/**
	 * This used to fail because the NoClass deserializer expected the latest class version. 
	 */
	@Test
	public void testDropEvolvedSchemaRollback() {
		//create data
		TestClassTiny t1 = new TestClassTiny();
		TestClassTiny2 t2 = new TestClassTiny2();
		pm.makePersistent(t1);
		pm.makePersistent(t2);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		//evolve
		ZooClass c1 = ZooSchema.locateClass(pm, TestClassTiny.class.getName());
		c1.defineField("hello", Long.TYPE);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		//delete
		ZooClass c2 = ZooSchema.locateClass(pm, TestClassTiny2.class.getName());
		c2.remove();
		c1 = ZooSchema.locateClass(pm, TestClassTiny.class.getName());
		c1.remove();

		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();
		
		Session s = Session.getSession(pm);
		for (Object mo: s.internalGetCache().getSchemata()) {
			if (JDOHelper.isDirty(mo)) {
				fail();
			}
		}
	}
	
	/**
	 * This used to fail because the NoClass deserializer expected the latest class version. 
	 */
	@Test
	public void testDropRenamedSchemaRollback() {
		//create data
		TestClassTiny t1 = new TestClassTiny();
		TestClassTiny2 t2 = new TestClassTiny2();
		pm.makePersistent(t1);
		pm.makePersistent(t2);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		//evolve
		ZooClass c1 = ZooSchema.locateClass(pm, TestClassTiny.class.getName());
		c1.defineField("hello", Long.TYPE);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		//delete
		ZooClass c2 = ZooSchema.locateClass(pm, TestClassTiny2.class.getName());
		c2.rename("TCT2");
		c1 = ZooSchema.locateClass(pm, TestClassTiny.class.getName());
		c1.rename("TCT");
		
		int n = 0;
		Session s = Session.getSession(pm);
		for (Object mo: s.internalGetCache().getSchemata()) {
			if (JDOHelper.isDirty(mo)) {
				n++;
			}
		}
		assertEquals(2, n);
		
		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();
		
		for (Object mo: s.internalGetCache().getSchemata()) {
			if (JDOHelper.isDirty(mo)) {
				fail();
			}
		}
	}
}

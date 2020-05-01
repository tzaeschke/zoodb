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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.Session;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.impl.PersistenceManagerImpl;
import org.zoodb.schema.ZooClass;
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
		ZooJdoHelper.schema(pm).addClass(TestClassTiny.class);
		ZooJdoHelper.schema(pm).addClass(TestClassTiny2.class);
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
		ZooClass c1 = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
		ZooClass c0 = ZooJdoHelper.schema(pm).getClass(ZooPC.class);
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
		ZooJdoHelper.schema(pm).defineEmptyClass(n1);
		ZooJdoHelper.schema(pm).defineEmptyClass(n2);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		assertNotNull(ZooJdoHelper.schema(pm).getClass(n1));
		assertNotNull(ZooJdoHelper.schema(pm).getClass(n2));
		
		Collection<ZooClass> cc = ZooJdoHelper.schema(pm).getAllClasses();
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
	public void testDropSchema() {
		//create data
		TestClassTiny t1 = new TestClassTiny();
		pm.makePersistent(t1);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//REMOVE sub-class (it's in the way)
		ZooJdoHelper.schema(pm).getClass(TestClassTiny2.class.getName()).remove();

		TestClassTiny t2 = new TestClassTiny();
		pm.makePersistent(t2);

		ZooClass c1 = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class.getName());
		c1.remove();
		try {
			c1.remove();
		} catch (IllegalStateException e) {
			//good
		}
		assertNull(ZooJdoHelper.schema(pm).getClass(TestClassTiny.class.getName()));
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		//delete
		try {
			c1.remove();
		} catch (IllegalStateException e) {
			//good
		}
		
		assertTrue(JDOHelper.isDeleted(t1));
		assertTrue(JDOHelper.isPersistent(t1));
		assertTrue(JDOHelper.isDeleted(t2));
		assertTrue(JDOHelper.isPersistent(t2));
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
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
		ZooClass c1 = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class.getName());
		c1.addField("hello", Long.TYPE);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		//delete
		ZooClass c2 = ZooJdoHelper.schema(pm).getClass(TestClassTiny2.class.getName());
		c2.remove();
		c1 = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class.getName());
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
		ZooClass c1 = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class.getName());
		c1.addField("hello", Long.TYPE);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		//delete
		ZooClass c2 = ZooJdoHelper.schema(pm).getClass(TestClassTiny2.class.getName());
		c2.remove();
		c1 = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class.getName());
		c1.remove();

		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();
		
		Session s = ((PersistenceManagerImpl)pm).getSession();
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
		ZooClass c1 = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class.getName());
		c1.addField("hello", Long.TYPE);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		//delete
		ZooClass c2 = ZooJdoHelper.schema(pm).getClass(TestClassTiny2.class.getName());
		c2.rename("TCT2");
		c1 = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class.getName());
		c1.rename("TCT");
		
		int n = 0;
		Session s = ((PersistenceManagerImpl)pm).getSession();
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

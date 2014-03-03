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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.spi.PersistenceCapable;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.impl.PersistenceManagerImpl;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.schema.ZooClass;
import org.zoodb.test.testutil.TestTools;

public class Test_084_SerailizationBugRefToPM {


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
		TestTools.defineSchema(ClassA.class, PureObject.class, HiddenPersRefToPM.class);
	}


	@AfterClass
	public static void afterClass() {
		TestTools.removeDb();
	}


	/**
	 * Test serialisation of PM via SCO. 
	 * 
	 * This used to fail because of NPE in OGT (see next test) and then because of StackOverflow.
	 */
	@Test
	public void testSerialization() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ClassRefToPM ref = new ClassRefToPM(pm);
		ClassA s = new ClassA(ref);
		pm.makePersistent(s);
		try {
			pm.currentTransaction().commit();
		} catch (JDOUserException e) {
			//good
		}
		TestTools.closePM();
	}
	
	/**
	 * Test serialisation. 
	 * 
	 * This used to fail because OGT fails on instances of class Object.
	 */
	@Test
	public void testSerializationPureObject() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		PureObject s = new PureObject();
		pm.makePersistent(s);
		Object oid = pm.getObjectId(s);
		pm.currentTransaction().commit();
		TestTools.closePM();

		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		PureObject s2 = (PureObject) pm.getObjectById(oid);
		assertNotNull(s2);
		assertNotNull(s2.getO());
		pm.currentTransaction().commit();
		pm.close();
	}
	
	/**
	 * Test serialisation of classes with obvious PM attributes. 
	 * 
	 * This used to fail because of NPE in OGT (see next test) and then because of StackOverflow.
	 */
	@Test
	public void testSerializationPM_Type() {
		try {
			TestTools.defineSchema(PersRefToPM.class);
			fail();
		} catch (JDOUserException e) {
			//good. defining classes with PM attributes should not be allowed
		}
	}
	
	/**
	 * Test serialisation of classes with non-obvious PM attributes. 
	 * 
	 * This used to fail because of NPE in OGT (see next test) and then because of StackOverflow.
	 */
	@Test
	public void testSerializationPM_Instance() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		HiddenPersRefToPM s = new HiddenPersRefToPM(pm);
		pm.makePersistent(s);
		try {
			pm.currentTransaction().commit();
		} catch (JDOUserException e) {
			//good
		}
		TestTools.closePM();
	}
	
	/**
	 * Test serialisation of classes with non-obvious PM attributes. 
	 * 
	 * This used to fail because of NPE in OGT (see next test) and then because of StackOverflow.
	 */
	@Test
	public void testSerializationPM_SchemaDeclaration() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooClass s = ZooJdoHelper.schema(pm).defineEmptyClass("X");
		try {
			s.addField("pmi", PersistenceManagerImpl.class);
			fail();
		} catch (JDOUserException e) {
			
		}
		try {
			s.addField("pm", PersistenceManager.class);
			fail();
		} catch (JDOUserException e) {
			
		}
		s.addField("pci", PersistenceCapableImpl.class);
		s.addField("pc", PersistenceCapable.class);
		s.addField("zpc", ZooPCImpl.class);
		pm.currentTransaction().commit();
		pm.close();
	}

}


class ClassA extends PersistenceCapableImpl {
	@SuppressWarnings("unused")
	private ClassRefToPM refSCO;

	protected ClassA() {
		// no-argument constructor for zoodb.
	}

	public ClassA(final ClassRefToPM ref) {
		this.refSCO = ref;
	}
}


class ClassRefToPM {
	@SuppressWarnings("unused")
	private PersistenceManager pm;
	
	protected ClassRefToPM() {
		// no-argument constructor for zoodb.
	}

	public ClassRefToPM(final PersistenceManager pm) {
		this.pm = pm;
	}
}

class PureObject extends PersistenceCapableImpl {
	private Object o = new Object();
	
	protected PureObject() {
		// no-argument constructor for zoodb.
	}

	public Object getO() {
		return o;
	}
}

class PersRefToPM extends PersistenceCapableImpl {
	@SuppressWarnings("unused")
	private PersistenceManager pm;
	
	protected PersRefToPM() {
		// no-argument constructor for zoodb.
	}

	public PersRefToPM(final PersistenceManager pm) {
		this.pm = pm;
	}
}

class HiddenPersRefToPM extends PersistenceCapableImpl {
	@SuppressWarnings("unused")
	private Object pm;
	
	protected HiddenPersRefToPM() {
		// no-argument constructor for zoodb.
	}

	public HiddenPersRefToPM(final PersistenceManager pm) {
		this.pm = pm;
	}
}



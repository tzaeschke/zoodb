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
import static org.junit.Assert.fail;

import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.spi.PersistenceCapable;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.api.impl.ZooPC;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.impl.PersistenceManagerImpl;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.schema.ZooClass;
import org.zoodb.test.testutil.TestTools;

public class Test_084_SerializationBugRefToPM {


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
		s.addField("zpc", ZooPC.class);
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



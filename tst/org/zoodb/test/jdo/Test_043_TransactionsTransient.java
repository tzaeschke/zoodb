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

import static org.junit.Assert.*;

import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.testutil.TestTools;

public class Test_043_TransactionsTransient {

	@BeforeClass
	public static void setUp() {
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
	}

	@Before
	public void before() {
		TestTools.dropInstances(TestClass.class);
	}

	@Test
	public void testActivationOutsideTx() {
		TestClass tc = new TestClass();
		tc.setInt(5);
		tc.getInt();
	}
	
	@Test
	public void testActivationOutsideTx2() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		pm.currentTransaction().commit();
		pm.close();
		
		TestClass tc = new TestClass();
		tc.setInt(5);
		tc.getInt();
	}
	
	
	@Test
	public void testTxInactiveFail() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		tc.setInt(5);
		pm.makePersistent(tc);
		
		pm.currentTransaction().commit();
		
		try {
			tc.setInt(6);
			fail();
		} catch (JDOUserException e) {
			// good, tx not active
		}
		
		try {
			tc.getInt();
			fail();
		} catch (JDOUserException e) {
			// good, tx not active
		}

		
		pm.close();
	}
	
	@Test
	public void testTxClosedFail() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		tc.setInt(5);
		pm.makePersistent(tc);
		
		pm.currentTransaction().commit();
		pm.close();
		
		try {
			tc.setInt(6);
			fail();
		} catch (JDOUserException e) {
			// good, tx closed
		}
		
		try {
			tc.getInt();
			fail();
		} catch (JDOUserException e) {
			// good, tx closed
		}
	}
	
	@Test
	public void testMakeTransient() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		tc.setInt(55);
		pm.makePersistent(tc);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//activate 
		tc.getInt();
		pm.makeTransient(tc);
		//should return null (JDO 3.0 p45)
		assertNull(pm.getObjectId(tc));
		assertNull(JDOHelper.getObjectId(tc));

		//check inside tx
		assertEquals(55, tc.getInt());
		tc.setInt(555);
		assertEquals(555, tc.getInt());
		assertFalse(JDOHelper.isPersistent(tc));
		
		//does commit work?
		try {
			pm.deletePersistent(tc);
			fail();
		} catch (JDOUserException e) {
			// tc does not belong to pm
		}

		pm.currentTransaction().commit();
		pm.close();
		
		//test get/set outside tx
		assertEquals(555, tc.getInt());
		tc.setInt(5555);
		assertEquals(5555, tc.getInt());
	}
	
	
	@Test
	public void testMakeTransientMakePersistent() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		tc.setInt(55);
		pm.makePersistent(tc);
		Object id1 = pm.getObjectId(tc);
		assertNotNull(id1);

		TestClass tcPers = new TestClass();
		pm.makePersistent(tcPers);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		pm.makeTransient(tc);

		pm.currentTransaction().commit();
		pm.close();
		pm.getPersistenceManagerFactory().close();
		
		//new transaction
		pm = TestTools.openPM();
		pm.currentTransaction().begin();

		pm.makePersistent(tc);
		Object id2 = pm.getObjectId(tc);
		assertNotNull(id2);
		assertFalse(id2.equals(id1));

		try {
			pm.makePersistent(tcPers);
			fail();
		} catch (JDOUserException e) {
			// cannot make persistent again, especially with different tx
		}
		
		try {
			pm.makeTransient(tcPers);
			fail();
		} catch (JDOUserException e) {
			// cannot make transient with different tx
		}
		
		pm.currentTransaction().commit();
		pm.close();

		
		try {
			tc.getInt();
			fail();
		} catch (JDOUserException e) {
			//should fail now
		}
	}
	
	
	@Test
	public void testMakeTransientErrors() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		pm.makePersistent(tc);
		try {
			pm.makeTransient(tc);
			fail();
		} catch (JDOUserException e) {
			// should fail if state is persistent-new: JDO 3.0 p59
		}

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();


		tc.setInt(123);
		try {
			pm.makeTransient(tc);
			fail();
		} catch (JDOUserException e) {
			// should fail if state is persistent-dirty: JDO 3.0 p59
		}
		
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();


		pm.deletePersistent(tc);
		try {
			pm.makeTransient(tc);
		} catch (JDOUserException e) {
			// should fail if state is 'deleted': JDO 3.0 p59ff
		}
		
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
	
	@After
	public void afterTest() {
		TestTools.closePM();
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
	}
}

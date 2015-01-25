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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jdo.JDOHelper;
import javax.jdo.ObjectState;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.test.testutil.TestTools;

public class Test_048_TransactionsOptions {

	@BeforeClass
	public static void beforeClass() {
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
	}

	@Before
	public void before() {
		TestTools.dropInstances(TestClass.class);
	}

	@After
	public void after() {
		TestTools.closePM();
	}
	
	@AfterClass
	public static void afterClass() {
		TestTools.removeDb();
	}
	
    @Test
    public void testRetainValuesDefFalse() {
        ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
        PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
        assertFalse(pmf.getRetainValues());
        
        PersistenceManager pm = pmf.getPersistenceManager();
        pm.currentTransaction().begin();
        assertFalse(pm.currentTransaction().getRetainValues());
        
        TestClass t1 = new TestClass();
        pm.makePersistent(t1);
        
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        assertEquals(ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL, JDOHelper.getObjectState(t1));
        t1.setInt(3);
        assertEquals(ObjectState.PERSISTENT_DIRTY, JDOHelper.getObjectState(t1));
        
        pm.currentTransaction().setRetainValues(true);
        assertTrue(pm.currentTransaction().getRetainValues());
        assertFalse(pmf.getRetainValues());
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();
        
        assertEquals(ObjectState.PERSISTENT_CLEAN, JDOHelper.getObjectState(t1));
        
        pm.currentTransaction().commit();
        pm.close();
        pmf.close();
    }

    @Test
    public void testRetainValuesDefTrue() {
        ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
        PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
        pmf.setRetainValues(true);
        assertTrue(pmf.getRetainValues());
        
        PersistenceManager pm = pmf.getPersistenceManager();
        pm.currentTransaction().begin();
        assertTrue(pm.currentTransaction().getRetainValues());
        
        TestClass t1 = new TestClass();
        pm.makePersistent(t1);
        
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        assertEquals(ObjectState.PERSISTENT_CLEAN, JDOHelper.getObjectState(t1));
        t1.setInt(3);
        assertEquals(ObjectState.PERSISTENT_DIRTY, JDOHelper.getObjectState(t1));
        
        pm.currentTransaction().setRetainValues(false);
        assertFalse(pm.currentTransaction().getRetainValues());
        assertTrue(pmf.getRetainValues());
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();
        
        assertEquals(ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL, JDOHelper.getObjectState(t1));
        
        pm.currentTransaction().commit();
        pm.close();
        pmf.close();
    }

	@Test
	public void testOptimistic() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		assertTrue(pm.currentTransaction().getOptimistic());
		
		//should work fine
		pm.currentTransaction().setOptimistic(true);
		
		//should fail
		try {
			pm.currentTransaction().setOptimistic(false);
			fail();
		} catch (UnsupportedOperationException e) {
			//good
		}
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}
	
	@Test
	public void testTxFeatures() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		assertFalse(pm.currentTransaction().getNontransactionalRead());
		//should work fine
		pm.currentTransaction().setNontransactionalRead(false);
		//should fail
		try {
			pm.currentTransaction().setNontransactionalRead(true);
			fail();
		} catch (UnsupportedOperationException e) {
			//good
		}
		
		assertFalse(pm.currentTransaction().getNontransactionalWrite());
		//should work fine
		pm.currentTransaction().setNontransactionalWrite(false);
		//should fail
		try {
			pm.currentTransaction().setNontransactionalWrite(true);
			fail();
		} catch (UnsupportedOperationException e) {
			//good
		}
		
		assertFalse(pm.currentTransaction().getRestoreValues());
		//should work fine
		pm.currentTransaction().setRestoreValues(false);
		//should fail
		try {
			pm.currentTransaction().setRestoreValues(true);
			fail();
		} catch (UnsupportedOperationException e) {
			//good
		}
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}

}

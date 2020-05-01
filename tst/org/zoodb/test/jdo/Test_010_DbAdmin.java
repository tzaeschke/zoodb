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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.test.testutil.TestTools;
import org.zoodb.tools.ZooHelper;
import org.zoodb.tools.impl.DataStoreManager;

public class Test_010_DbAdmin {

	private static final String dbName1 = "TestDb1";
	private static final String dbName2 = "TestDb2";

	@BeforeClass
	public static void setUpClass() {
		//remove all databases
		tearDownClass();
	}

	private static DataStoreManager dsm() {
		return ZooHelper.getDataStoreManager();
	}

	@Test
	public void testCreateDb() {
		//now files stuff should fail
		//try {
		assertFalse(dsm().removeDb(dbName1));
//			fail();
//		} catch (JDOUserException e) {
//			//ok
//		}


		dsm().createDb(dbName1);

		//now files stuff should fail
		try {
			dsm().createDb(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}

		assertTrue(dsm().removeDb(dbName1));

		//now files stuff should fail
		assertFalse(dsm().removeDb(dbName1));

		//try again
		dsm().createDb(dbName1);
		assertTrue(dsm().removeDb(dbName1));
	}

	@Test
	public void testDoubleDbCreation() {
		//test files creation
		dsm().createDb(dbName1);
		try {
			dsm().createDb(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}
		dsm().createDb(dbName2);


		//test files removal
		assertTrue(dsm().removeDb(dbName1));
		assertFalse(dsm().removeDb(dbName1));
//		try {
//			fail();
//		} catch (JDOUserException e) {
//			//ok
//		}
		dsm().removeDb(dbName2);
	}


	@Test
	public void testDeletionWhileOpen() {
		//test files creation
		dsm().createDb(dbName1);
		
		PersistenceManager pm = TestTools.openPM(dbName1);
		pm.currentTransaction().begin();
		
		try {
			dsm().removeDb(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}

		pm.currentTransaction().commit();
		TestTools.closePM();

		//test files removal
		assertTrue(dsm().removeDb(dbName1));
	}


	@Test
	public void testNonDefaultLocation() {
	    //Create a folder at %USER_HOME%/zoodbTest
        String fullPath = dsm().getDefaultDbFolder() + "Test" + File.separator + dbName1;
	    
        //test set-up: ensure that databases do not exist.
        if (dsm().dbExists(dbName1)) {
        	dsm().removeDb(dbName1);
        }
        if (dsm().dbExists(fullPath)) {
            dsm().removeDb(fullPath);
        }
        
	    //Folder should be empty now
        assertFalse(dsm().dbExists(dbName1));
        assertFalse(dsm().dbExists(fullPath));

	    //create database
        dsm().createDb(dbName1);
        dsm().createDb(fullPath);
	    
        //check location
        assertTrue(dsm().dbExists(dbName1));
        assertTrue(dsm().dbExists(fullPath));
        
        //Test accessibility of alternative DB
        ZooJdoProperties cfg = new ZooJdoProperties(fullPath);
        PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(cfg);
        PersistenceManager pm = pmf.getPersistenceManager();
        pm.currentTransaction().begin();
        pm.currentTransaction().commit();
        pm.close();
        pmf.close();
        
        dsm().removeDb(dbName1);
        dsm().removeDb(fullPath);
        assertFalse(dsm().dbExists(dbName1));
        assertFalse(dsm().dbExists(fullPath));
	}
	
	@AfterClass
	public static void tearDownClass() {
		try {
			dsm().removeDb(dbName1);
			//System.out.println("Removed files: " + dbName1);
		} catch (JDOUserException e) {
			//ok
		}
		try {
			dsm().removeDb(dbName2);
			//System.out.println("Removed files: " + dbName2);
		} catch (JDOUserException e) {
			//ok
		}
	}
}

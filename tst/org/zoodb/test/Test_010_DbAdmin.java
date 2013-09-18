/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
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
import org.zoodb.jdo.api.DataStoreManager;
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.tools.ZooHelper;

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
		try {
			dsm().removeDb(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}


		dsm().createDb(dbName1);

		//now files stuff should fail
		try {
			dsm().createDb(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}

		dsm().removeDb(dbName1);

		//now files stuff should fail
		try {
			dsm().removeDb(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}

		//try again
		dsm().createDb(dbName1);
		dsm().removeDb(dbName1);
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
		dsm().removeDb(dbName1);
		try {
			dsm().removeDb(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}
		dsm().removeDb(dbName2);
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
			System.out.println("Removed files: " + dbName1);
		} catch (JDOUserException e) {
			//ok
		}
		try {
			dsm().removeDb(dbName2);
			System.out.println("Removed files: " + dbName2);
		} catch (JDOUserException e) {
			//ok
		}
	}
}

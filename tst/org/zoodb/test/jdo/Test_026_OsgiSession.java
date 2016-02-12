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

import javax.jdo.JDOFatalUserException;
import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.impl.PersistenceManagerFactoryImpl;
import org.zoodb.test.testutil.TestTools;
import org.zoodb.tools.ZooConfig;
import org.zoodb.tools.ZooHelper;

/**
 * Test OSGI support. 
 * 
 * @author tilmann
 */
public class Test_026_OsgiSession {

	@BeforeClass
	public static void beforeClass() {
        TestTools.createDb();
    }
 
	@After
	public void after() {
    	ZooConfig.setDefaults();
	}
	
    @AfterClass
    public static void tearDown() {
        TestTools.removeDb();
    }
	
    
    @Test
    public void testDefaulPmf() {
    	PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory("random name");
    	assertNotNull(pmf);
    	assertEquals(PersistenceManagerFactoryImpl.class, pmf.getClass());
    	//TODO what are we expecting here.... ?
    }
	
    
    @Test(expected=Exception.class)
    public void testPmCreationFailsOnBadName() {
    	PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory("FredIsHungry");
    	pmf.getPersistenceManager();
    }

	
    @Test
    public void testPmCreationWorksWithDbName() {
    	PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(TestTools.getDbName());
       	try {
    		pmf.getPersistenceManager();
    		fail();
    	} catch (JDOUserException e) {
    		//good
    	}
    	pmf.setConnectionURL(TestTools.getDbName());
    	PersistenceManager pm = pmf.getPersistenceManager();
    	pm.close();
    }

	
    @Test(expected=JDOFatalUserException.class)
    public void testPmfFailsOnEmptyName() {
   		JDOHelper.getPersistenceManagerFactory("");
    }

    
    @Test(expected=JDOFatalUserException.class)
    public void testPmfFailsOnNull() {
   		JDOHelper.getPersistenceManagerFactory((String)null);
    }
    
	
    
    @Test
    public void testInMemoryPmf() {
    	ZooConfig.setFileManager(ZooConfig.FILE_MGR_IN_MEMORY);
    	String dbName = "myDbOsgi";
    	ZooHelper.getDataStoreManager().createDb(dbName);
    	PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(dbName);
    	assertNotNull(pmf);
    	assertEquals(PersistenceManagerFactoryImpl.class, pmf.getClass());
    	try {
    		pmf.getPersistenceManager();
    		fail();
    	} catch (JDOUserException e) {
    		//good
    	}
    	pmf.setConnectionURL(dbName);
    	PersistenceManager pm = pmf.getPersistenceManager();
    	pm.close();
    }

}

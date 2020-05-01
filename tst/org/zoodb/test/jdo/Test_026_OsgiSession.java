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

    
    @Test
    public void testGetSetPU() {
    	String puName = "random name";
    	String puName2 = "random name2";
    	PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(puName);
    	
    	assertEquals(puName, pmf.getPersistenceUnitName());
    	pmf.setPersistenceUnitName(puName2);
    	assertEquals(puName2, pmf.getPersistenceUnitName());
    }
}

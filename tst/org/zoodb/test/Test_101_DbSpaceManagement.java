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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.ZooConfig;
import org.zoodb.jdo.api.impl.DBStatistics;
import org.zoodb.test.testutil.TestTools;
import org.zoodb.tools.ZooHelper;

public class Test_101_DbSpaceManagement {

	@Before
	public void before() {
		TestTools.removeDb();
		//Config.setFileManager(Config.FILE_MGR_IN_MEMORY);
		//Config.setFileProcessor(Config.FILE_PAF_BB_MAPPED_PAGE);
		//Config.setFilePageSize(Config.FILE_PAGE_SIZE_DEFAULT * 4);
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
		TestTools.defineSchema(TestClassTiny.class);
		DBStatistics.enable(true);
	}

	@After
	public void after() {
		TestTools.closePM();
		DBStatistics.enable(false);
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
		ZooConfig.setFilePageSize(ZooConfig.FILE_PAGE_SIZE_DEFAULT);
		DBStatistics.enable(false);
	}

	
	/**
	 * We test how many pages get written if we do not actually 
	 * change anything before committing.
	 */
	@Test
	public void testCommitOverhead() {
		File f = new File(TestTools.getDbFileName());
		long len1 = f.length();

		//open session
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		pm.currentTransaction().commit();
		TestTools.closePM();

		assertEquals(len1, f.length());
	}

	/**
	 * We test how many pages get written if we do not actually 
	 * change anything before committing but perform empty deletion
	 * attempts.
	 */
	@Test
	public void testCommitOverheadAfterEmptyDelete() {
		File f = new File(TestTools.getDbFileName());
		long len1 = f.length();

		//open session
		PersistenceManager pm = TestTools.openPM();

		DBStatistics dbs = ZooHelper.getStatistics(pm);
		int pwc0 = dbs.getStoragePageWriteCount();

		
		// 1. try Query.deletePersistentAll()
		pm.currentTransaction().begin();
		deletePersistentAll(pm, TestClass.class);
		pm.currentTransaction().commit();
		
		//2. try PersistenceManager.delete(Extent.iterator().next()) with batches 
		pm.currentTransaction().begin();
		deleteAllBatched(pm, TestClass.class);
		pm.currentTransaction().commit();
			
		int pwc2 = dbs.getStoragePageWriteCount();
		assertEquals(pwc0, pwc2);

		TestTools.closePM();

		assertEquals(len1, f.length());
	}
	
	
	private void deletePersistentAll(PersistenceManager pm, Class<?> clazz) {
		pm.newQuery(pm.getExtent(clazz,false)).deletePersistentAll();
	}
	
	private void deleteAllBatched(PersistenceManager pm, Class<?> clazz) {
	    int batchSize = 10000;
            int commitctr = 0;
            Extent<?> extent = pm.getExtent(clazz,false);
            Iterator<?> it = extent.iterator();
            while(it.hasNext()){
                pm.deletePersistent(it.next());
                if ( batchSize > 0  &&  ++commitctr >= batchSize){
                    commitctr = 0;
                    pm.currentTransaction().commit();
                    pm.currentTransaction().begin();
                }
            }
            extent.closeAll();
 	}
	
	
	/**
	 * We test how many pages get written if we do not actually 
	 * change anything before committing but perform empty deletion
	 * attempts.
	 */
	@Test
	public void testCommitOverheadAfterEmptyReadAndQuery() {
		//open session
		PersistenceManager pm = TestTools.openPM();

		// 1. try Query.deletePersistentAll()
		pm.currentTransaction().begin();
		TestClass tc = null;
		for (int i = 0; i < 10000; i++) {
			tc = new TestClass();
			pm.makePersistent(tc);
		}
		pm.currentTransaction().commit();
		

		DBStatistics dbs = ZooHelper.getStatistics(pm);
		int pwc0 = dbs.getStoragePageWriteCount();
        File f = new File(TestTools.getDbFileName());
        long len1 = f.length();

		//try different types of read 
		pm.currentTransaction().begin();
		for (int i = 0; i < 5; i++) {
			//refresh one object
			pm.refresh(tc);
			//queries
			Collection<?> c = (Collection<?>) pm.newQuery(pm.getExtent(TestClass.class)).execute();
			for (Object o: c) {
				assertNotNull(o);
			}
			c = (Collection<?>) pm.newQuery("select from " + TestClass.class.getName()).execute();
			for (Object o: c) {
				assertNotNull(o);
			}
			if (i % 2 == 0) {
				pm.currentTransaction().commit();
			} else {
				pm.currentTransaction().rollback();
			}
			pm.currentTransaction().begin();
		}
		pm.currentTransaction().commit();

		//check that nothing got written
		int pwc2 = dbs.getStoragePageWriteCount();
		assertEquals(pwc0, pwc2);

		TestTools.closePM();

		assertEquals(len1, f.length());
	}
	
	

}

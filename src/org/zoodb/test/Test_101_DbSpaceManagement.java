package org.zoodb.test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

import java.io.File;
import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.internal.Config;
import org.zoodb.jdo.internal.Session;
import org.zoodb.test.util.TestTools;

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
	}

	@After
	public void after() {
		TestTools.closePM();
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
		Config.setFilePageSize(Config.FILE_PAGE_SIZE_DEFAULT);
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

		// 1. try Query.deletePersistentAll()
		pm.currentTransaction().begin();
		deletePersistentAll(pm, TestClass.class);
		pm.currentTransaction().commit();
		
		//2. try PersistenceManager.delete(Extent.iterator().next()) with batches 
		pm.currentTransaction().begin();
		deleteAllBatched(pm, TestClass.class);
		pm.currentTransaction().commit();
			
		TestTools.closePM();

		assertEquals(len1, f.length());
		
		//TODO
		Session s = (Session) pm.getDataStoreConnection().getNativeConnection();
		fail();
		//s.getPrimaryNode().getStats().getPageWriteCount();
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
}

package org.zoodb.test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Collection;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.internal.Config;

public class Test_100_FreeSpaceManager {

	private static final String DB_NAME = "TestDb";
	
	@Before
	public void before() {
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
	
	@Test
	public void testObjectsRollback() {
		final int MAX = 1000;
		
		File f = new File(TestTools.getDbFileName());
		long len1 = f.length();

		//create some object
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		for (int i = 0; i < MAX; i++) {
			TestClass tc = new TestClass();
		
			pm.makePersistent(tc);
			//Object oidP = pm.getObjectId(tc);
		}
		pm.currentTransaction().rollback();
		TestTools.closePM();

		assertEquals(len1, f.length());
	}
	
	@Test
	public void testObjectsReusePagesDeleted() {
		final int MAX = 1000;
		
		File f = new File(TestTools.getDbFileName());

		//First, create objects
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		for (int i = 0; i < MAX; i++) {
			TestClass tc = new TestClass();
		
			pm.makePersistent(tc);
			//Object oidP = pm.getObjectId(tc);
		}
		pm.currentTransaction().commit();
		
		
		pm.currentTransaction().begin();
		//now delete them
		Collection<TestClass> col = (Collection<TestClass>) pm.newQuery(TestClass.class).execute();
		for (TestClass tc: col) {
			pm.deletePersistent(tc);
		}
		pm.currentTransaction().commit();
		TestTools.closePM();


		//check length
		long len1 = f.length();

		//create objects
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		for (int i = 0; i < MAX; i++) {
			TestClass tc = new TestClass();
		
			pm.makePersistent(tc);
			//Object oidP = pm.getObjectId(tc);
		}
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		//check that the new Objects reused previous pages
		//w/o FSM, the values were 274713 vs 401689
		//w/o #2 380920 / 524280
		assertEquals(len1/1024, f.length()/1024);
	}

	@Test
	public void testObjectsReusePagesDirtyObjects() {
		final int MAX = 1000;
		
		File f = new File(TestTools.getDbFileName());

		//First, create objects
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		for (int i = 0; i < MAX; i++) {
			TestClass tc = new TestClass();
		
			pm.makePersistent(tc);
			//Object oidP = pm.getObjectId(tc);
		}
		pm.currentTransaction().commit();
		TestTools.closePM();

		//now make them dirty
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		Collection<TestClass> col1 = (Collection<TestClass>) pm.newQuery(TestClass.class).execute();
		for (TestClass tc: col1) {
			JDOHelper.makeDirty(tc, null);
		}
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		//check length
		long len1 = f.length();
		System.out.println("Now we measure");

		//now make them dirty again, this should reuse pages of the original objects
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		Collection<TestClass> col = (Collection<TestClass>) pm.newQuery(TestClass.class).execute();
		for (TestClass tc: col) {
			JDOHelper.makeDirty(tc, null);
		}
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		//check that the new Objects reused previous pages
		//w/o FSM, the values were 274713 vs 401689
		//w/o #2 380920 / 524280
		assertEquals(len1/1024, f.length()/1024);
	}

	@Test
	public void testObjectsReusePagesAfterCommitOnly() {
		final int MAX = 1000;
		
		File f = new File(TestTools.getDbFileName());

		//First, create objects
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		for (int i = 0; i < MAX; i++) {
			TestClass tc = new TestClass();
			tc.setInt(14);
			pm.makePersistent(tc);
			//Object oidP = pm.getObjectId(tc);
		}
		pm.currentTransaction().commit();
		

		//check length
		long len1 = f.length();

		//now delete them and create new ones.
		pm.currentTransaction().begin();
		Collection<TestClass> col = (Collection<TestClass>) pm.newQuery(TestClass.class).execute();
		for (TestClass tc: col) {
			pm.deletePersistent(tc);
		}

		//create objects
		for (int i = 0; i < MAX; i++) {
			TestClass tc = new TestClass();
			tc.setInt(18);
			pm.makePersistent(tc);
			//Object oidP = pm.getObjectId(tc);
		}
		pm.currentTransaction().commit();

		//ensure that the new object got written and the previous one disappeared from the indices
		pm.currentTransaction().begin();
		col = (Collection<TestClass>) pm.newQuery(TestClass.class).execute();
		for (TestClass tc: col) {
			assertEquals(18, tc.getInt());
			pm.deletePersistent(tc);
		}
		pm.currentTransaction().rollback();
		
		
		TestTools.closePM();
		
		//check that the new Objects did NOT reuse previous pages
		//w/o FSM, the values were 258329 vs 381209
		assertTrue("l1=" + len1/1024 + " l2=" + f.length()/1024, len1*1.5 < f.length());
	}

	
	/**
	 * Test with multi-page objects
	 */
	@Test
	public void testObjectsReusePagesWithLargeObjects() {
		final int MAX = 10;
		final int SIZE = 100000;
		byte[] ba = new byte[SIZE];
		
		
		File f = new File(TestTools.getDbFileName());

		//First, create objects
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		for (int i = 0; i < MAX; i++) {
			TestClass tc = new TestClass();
			tc.setByteArray(ba);
			pm.makePersistent(tc);
			//Object oidP = pm.getObjectId(tc);
		}
		pm.currentTransaction().commit();
		
		
		pm.currentTransaction().begin();
		//now delete them
		Collection<TestClass> col = (Collection<TestClass>) pm.newQuery(TestClass.class).execute();
		for (TestClass tc: col) {
			pm.deletePersistent(tc);
		}
		pm.currentTransaction().commit();
		TestTools.closePM();


		//check length
		long len1 = f.length();

		//create objects
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		for (int i = 0; i < MAX; i++) {
			TestClass tc = new TestClass();
			tc.setByteArray(ba);
			pm.makePersistent(tc);
			//Object oidP = pm.getObjectId(tc);
		}
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		//check that the new Objects reused previous pages
		//w/o FSM, the values were 1179929 vs 2212121
		assertEquals(len1/1024, f.length()/1024);
	}


	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb(DB_NAME);
		Config.setFilePageSize(Config.FILE_PAGE_SIZE_DEFAULT);
	}
	
}

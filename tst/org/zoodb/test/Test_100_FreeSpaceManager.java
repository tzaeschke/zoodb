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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Collection;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.ZooConfig;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.test.testutil.TestTools;
import org.zoodb.tools.ZooCheckDb;

public class Test_100_FreeSpaceManager {

	@Before
	public void before() {
		TestTools.removeDb();
		//Config.setFileManager(Config.FILE_MGR_IN_MEMORY);
		//Config.setFileProcessor(Config.FILE_PAF_BB_MAPPED_PAGE);
		//Config.setFilePageSize(Config.FILE_PAGE_SIZE_DEFAULT * 4);
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
		//TestTools.defineSchema(TestClassTiny.class);
	}

	@After
	public void after() {
		TestTools.closePM();
		ZooCheckDb.main(new String[]{});
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
	
	@SuppressWarnings("unchecked")
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
		int ps = ZooConfig.getFilePageSize();
//		System.out.println("l1=" + len1/ps + " l2=" + f.length()/ps);
		assertTrue("l1=" + len1/ps + " l2=" + f.length()/ps, len1*1.1 > f.length());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testObjectsReusePagesDeletedMulti() {
		final int MAX = 2000;
		final int MAX_ITER = 50;
		
		File f = new File(TestTools.getDbFileName());

		long len1 = -1;
		
		for (int j = 0; j < MAX_ITER; j++) {
			//First, create objects
			PersistenceManager pm = TestTools.openPM();
			pm.currentTransaction().begin();
			for (int i = 0; i < MAX; i++) {
				TestClass tc = new TestClass();
				pm.makePersistent(tc);
			}
			pm.currentTransaction().commit();

			//also close tx every now and then
			if (j % 3 == 0) {
				TestTools.closePM();
				pm = TestTools.openPM();
			}
			
			pm.currentTransaction().begin();
			//now delete them
			Collection<TestClass> col = 
				(Collection<TestClass>) pm.newQuery(TestClass.class).execute();
			for (TestClass tc: col) {
				pm.deletePersistent(tc);
			}
			pm.currentTransaction().commit();
			TestTools.closePM();
	
	
			//check length only from 3rd iteration on...
			if (j == 3) {
				len1 = f.length();
			}
		}

		//check that the new Objects reused previous pages
		int ps = ZooConfig.getFilePageSize();
//		System.out.println("l1=" + len1/ps + " l2=" + f.length()/ps);
		assertTrue("l1=" + len1/ps + " l2=" + f.length()/ps, 
				(len1*1.1 > f.length()) || (f.length()/ps - len1/ps < 20));
	}

	@Test
	public void testObjectsReusePagesDroppedMulti() {
		final int MAX = 2000;
		final int MAX_ITER = 50;
		
		File f = new File(TestTools.getDbFileName());

		long len1 = -1;
		
		for (int j = 0; j < MAX_ITER; j++) {
			//First, create objects
			PersistenceManager pm = TestTools.openPM();
			pm.currentTransaction().begin();
			for (int i = 0; i < MAX; i++) {
				TestClass tc = new TestClass();
				pm.makePersistent(tc);
			}
			pm.currentTransaction().commit();

			//also close tx every now and then
			if (j % 3 == 0) {
				TestTools.closePM();
				pm = TestTools.openPM();
			}
			
			pm.currentTransaction().begin();
			//now delete them
			ZooSchema.locateClass(pm, TestClass.class).dropInstances();
			pm.currentTransaction().commit();
			TestTools.closePM();
	
	
			//check length only from 3rd iteration on...
			if (j == 3) {
				len1 = f.length();
			}
		}

		//check that the new Objects reused previous pages
		int ps = ZooConfig.getFilePageSize();
//		System.out.println("l1=" + len1/ps + " l2=" + f.length()/ps);
		assertTrue("l1=" + len1/ps + " l2=" + f.length()/ps, 
				(len1*1.1 > f.length()) || (f.length()/ps - len1/ps < 20));
	}

	
	@Test
	public void testObjectsReusePagesDeletedSchema() {
		TestTools.removeSchema(TestClass.class);
		
		final int MAX = 2000;
		final int MAX_ITER = 50;
		long l = 0;
		File f = new File(TestTools.getDbFileName());

		long len1 = -1;
		
		for (int j = 0; j < MAX_ITER; j++) {
			//First, create objects
			PersistenceManager pm = TestTools.openPM();
			pm.currentTransaction().begin();
			ZooSchema.defineClass(pm, TestClass.class);
			ZooSchema.locateClass(pm, TestClass.class).createIndex("_int" , false);
			ZooSchema.locateClass(pm, TestClass.class).createIndex("_long" , true);
			for (int i = 0; i < MAX; i++) {
				TestClass tc = new TestClass();
				tc.setLong(l++);
				pm.makePersistent(tc);
			}
			pm.currentTransaction().commit();

			//also close tx every now and then
			if (j % 3 == 0) {
				TestTools.closePM();
				pm = TestTools.openPM();
			}
			
			pm.currentTransaction().begin();
			//now delete them
			ZooSchema.locateClass(pm, TestClass.class).dropInstances();
			ZooSchema.locateClass(pm, TestClass.class).removeIndex("_int");
			//we try to drop _long implicitly.
			ZooSchema.locateClass(pm, TestClass.class).remove();
			pm.currentTransaction().commit();
			TestTools.closePM();
	
	
			//check length only from 3rd iteration on...
			if (j == 3) {
				len1 = f.length();
			}
		}

		//check that the new Objects reused previous pages
		int ps = ZooConfig.getFilePageSize();
//		System.out.println("l1=" + len1/ps + " l2=" + f.length()/ps);
		assertTrue("l1=" + len1/ps + " l2=" + f.length()/ps, 
				(len1*1.1 > f.length()) || (f.length()/ps - len1/ps < 20));
	}

	
	@SuppressWarnings("unchecked")
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
		int ps = ZooConfig.getFilePageSize();
//		assertEquals(len1/1024, f.length()/1024);
		assertTrue("l1=" + len1/ps + " l2=" + f.length()/ps, len1*1.1 > f.length());
	}

	@SuppressWarnings("unchecked")
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
		int ps = ZooConfig.getFilePageSize();
		assertTrue("l1=" + len1/ps + " l2=" + f.length()/ps, len1*1.4 < f.length());
	}

	
	/**
	 * Test with multi-page objects
	 */
	@SuppressWarnings("unchecked")
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
		//assertEquals(len1/1024, f.length()/1024);
		int ps = ZooConfig.getFilePageSize();
		assertTrue("l1=" + len1/ps + " l2=" + f.length()/ps, len1*1.1 > f.length());
	}


	/**
	 * Test with multi-page objects
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testObjectsDoNotReusePagesWithOverlappingObjects() {
		final int MAX = 100;
		final int SIZE = 10000;  //multi-page object must be likely
		int nTotal = 0;
		byte[] ba1 = new byte[SIZE];
		byte[] ba2 = new byte[SIZE];
		for (int i = 0; i < SIZE; i++) {
			ba1[i] = 11;
			ba2[i] = 13;
		}
		
		
		//First, create objects
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		for (int i = 0; i < MAX; i++) {
			TestClass tc = new TestClass();
			tc.setByteArray(ba1);
			pm.makePersistent(tc);
			nTotal++;
			//Object oidP = pm.getObjectId(tc);
		}
		pm.currentTransaction().commit();
		
		pm.currentTransaction().begin();
		//now delete them
		Collection<TestClass> col = (Collection<TestClass>) pm.newQuery(TestClass.class).execute();
		int n = 0;
		for (TestClass tc: col) {
			if ((n++ % 2) == 0) {
				pm.deletePersistent(tc);
				nTotal--;
			}
		}
		pm.currentTransaction().commit();
		TestTools.closePM();

		//create objects
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		for (int i = 0; i < MAX; i++) {
			TestClass tc = new TestClass();
			tc.setByteArray(ba2);
			pm.makePersistent(tc);
			nTotal++;
		}
		pm.currentTransaction().commit();
		TestTools.closePM();

		// now check objects
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		col = (Collection<TestClass>) pm.newQuery(TestClass.class).execute();
		n = 0;
		for (TestClass tc: col) {
			n++;
			byte[] ba = tc.getBytaArray();
			int b0 = ba[0];
			for (byte b2: ba) {
				assertEquals(b0, b2);
			}
		}
		assertEquals(nTotal, n);
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
		ZooConfig.setFilePageSize(ZooConfig.FILE_PAGE_SIZE_DEFAULT);
	}
	
}

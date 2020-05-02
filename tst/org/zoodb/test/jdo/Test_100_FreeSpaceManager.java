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
import org.zoodb.api.DBArrayList;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.test.api.PersistentDummyImpl;
import org.zoodb.test.testutil.TestTools;
import org.zoodb.tools.ZooCheckDb;
import org.zoodb.tools.ZooConfig;

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
		ZooCheckDb.enableStringOutput();
		ZooCheckDb.main();
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
			ZooJdoHelper.schema(pm).getClass(TestClass.class).dropInstances();
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
			ZooJdoHelper.schema(pm).addClass(TestClass.class);
			ZooJdoHelper.schema(pm).getClass(TestClass.class).createIndex("_int" , false);
			ZooJdoHelper.schema(pm).getClass(TestClass.class).createIndex("_long" , true);
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
			ZooJdoHelper.schema(pm).getClass(TestClass.class).dropInstances();
			ZooJdoHelper.schema(pm).getClass(TestClass.class).removeIndex("_int");
			//we try to drop _long implicitly.
			ZooJdoHelper.schema(pm).getClass(TestClass.class).remove();
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
	
    /**
     * test batch loading.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testFsmBug1_BatchLoading() {
        //System.out.println("Batch-test");
    	TestTools.defineSchema(PersistentDummyImpl.class);
    	
        PersistenceManager pm = null;
        Object oid = null;

        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        DBArrayList<Object> dbv = new DBArrayList<Object>();
//        dbv.add("TestString");
        for (int i = 0 ; i < 100; i++) {
            dbv.add(new PersistentDummyImpl());
        }
//        dbv.add("TestString2");
        pm.makePersistent(dbv);
        oid = pm.getObjectId(dbv);
        pm.currentTransaction().commit(); 
        pm.close();
        pm = null;

//        pm = TestTools.openPM();
//        pm.currentTransaction().begin();
//        dbv = (DBArrayList<Object>) pm.getObjectById(oid);
        long t1 = System.currentTimeMillis();
//        for (Object o: dbv) {
//            o.hashCode();
//        }
        long t2 = System.currentTimeMillis();
//        System.out.println("NORMAL: " + (t2 - t1));
//        pm.currentTransaction().commit(); 
//        pm.close();
//        pm = null;
//
//        pm = TestTools.openPM();
//        pm.currentTransaction().begin();
//        dbv = (DBArrayList<Object>) pm.getObjectById(oid);
//        t1 = System.currentTimeMillis();
//        dbv.setBatchSize(1000);
//        for (Object o: dbv) {
//            o.hashCode();
//        }
//        t2 = System.currentTimeMillis();
//        System.out.println("BATCHED: " + (t2 - t1));
//        pm.currentTransaction().commit(); 
//        pm.close();
//        pm = null;
//
//        //Close the store and load the stuff
//        pm = TestTools.openPM();
//        pm.currentTransaction().begin();
//        dbv = (DBArrayList<Object>) pm.getObjectById(oid);
//        t1 = System.currentTimeMillis();
//        dbv.setBatchSize(1);
//        for (Object o: dbv) {
//            o.hashCode();
//        }
//        t2 = System.currentTimeMillis();
//        System.out.println("NORMAL: " + (t2 - t1));
//        pm.currentTransaction().commit(); 
//        pm.close();
//        pm = null;
//
//        //Close the store and load the stuff
//        pm = TestTools.openPM();
//        pm.currentTransaction().begin();
//        dbv = (DBArrayList<Object>) pm.getObjectById(oid);
//        t1 = System.currentTimeMillis();
//        dbv.setBatchSize(0);
//        for (Object o: dbv) {
//            o.hashCode();
//        }
//        t2 = System.currentTimeMillis();
//        System.out.println("BATCHED: " + (t2 - t1));
//        pm.currentTransaction().commit(); 
//        pm.close();
//        pm = null;

        //Close the store, load the stuff and test with transient object
        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        dbv = (DBArrayList<Object>) pm.getObjectById(oid);
        PersistentDummyImpl dummyTrans = new PersistentDummyImpl();
        dbv.add(13, dummyTrans);
        t1 = System.currentTimeMillis();
        dbv.setBatchSize(0);
        for (Object o: dbv) {
            o.hashCode();
        }
        t2 = System.currentTimeMillis();
        assertEquals(dummyTrans, dbv.get(13));
        System.out.println("BATCHED: " + (t2 - t1));
        pm.currentTransaction().commit(); 
        pm.close();
        pm = null;

        //Close the store, load the stuff and test with modified object
        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        dbv = (DBArrayList<Object>) pm.getObjectById(oid);
        ((PersistentDummyImpl)dbv.get(18)).setData(new byte[]{15});
        t1 = System.currentTimeMillis();
        dbv.setBatchSize(0);
        for (Object o: dbv) {
            o.hashCode();
        }
        t2 = System.currentTimeMillis();
        assertEquals(15, ((PersistentDummyImpl)dbv.get(18)).getData()[0]);
        System.out.println("BATCHED but dirty: " + (t2 - t1));
        pm.currentTransaction().rollback();
        TestTools.closePM();

        //TODO use setBatch() also for all other tests to verify batch loading!
        //Or call these tests here again, outside the store.!
    }
    
    /**
     * test batch loading.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testFsmBug1_BatchLoading2() {
    	//System.out.println("Batch-test 2");
    	TestTools.defineSchema(PersistentDummyImpl.class);
    	
        PersistenceManager pm = null;
        Object oid = null;
        try {
    		pm = TestTools.openPM();
    		pm.currentTransaction().begin();
            DBArrayList<Object> dbv = new DBArrayList<Object>();
            for (int i = 0 ; i < 120; i++) {
                dbv.add(new PersistentDummyImpl());
            }
            pm.makePersistent(dbv);
            oid = pm.getObjectId(dbv);
            pm.currentTransaction().commit(); 
            pm.close();
            pm = null;
        
    		pm = TestTools.openPM();
    		pm.currentTransaction().begin();
            dbv = (DBArrayList<Object>) pm.getObjectById(oid);
            dbv.setBatchSize(110);
            for (Object o: dbv) {
                o.getClass();
            }
            pm.currentTransaction().commit(); 
        } finally {
            if (pm != null) {
            	if (pm.currentTransaction().isActive()) {
            		pm.currentTransaction().rollback();
            	}
                pm.close();
            }
        }
    }

	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
		ZooConfig.setFilePageSize(ZooConfig.FILE_PAGE_SIZE_DEFAULT);
	}
	
}

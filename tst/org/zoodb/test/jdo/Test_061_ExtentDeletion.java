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
import static org.junit.Assert.assertNotNull;

import java.util.Collection;
import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.testutil.TestTools;

public class Test_061_ExtentDeletion {

	@BeforeClass
	public static void setUp() {
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class, TestClassTiny.class, TestClassTiny2.class);
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
	}
	
	@Before
	public void beforeTest() {
	    PersistenceManager pm = TestTools.openPM();
	    pm.currentTransaction().begin();
        pm.newQuery(pm.getExtent(TestClass.class)).deletePersistentAll();
        pm.newQuery(pm.getExtent(TestClassTiny.class)).deletePersistentAll();
        pm.newQuery(pm.getExtent(TestClassTiny2.class)).deletePersistentAll();
        pm.currentTransaction().commit();
        TestTools.closePM();
	}

	@After
	public void afterTest() {
		TestTools.closePM();
	}
	
	private void count(PersistenceManager pm, Class<?> cls, int nExp) {
		int nExt = 0;
		for (Object o: pm.getExtent(cls, false)) {
			assertNotNull(o);
			nExt++;
		}
		
		int nQ = 0;
		Collection<?> c = (Collection<?>) pm.newQuery(pm.getExtent(cls, false)).execute();
		for (Object o: c) {
			assertNotNull(o);
			nQ++;
		}
		
		assertEquals(nExp, nExt);
		assertEquals(nExp, nQ);
	}
	
	private void checkCount(PersistenceManager pm, int nTC, int nTCT, int nTCT2) {
		count( pm, TestClass.class, nTC);
		count( pm, TestClassTiny.class, nTCT);
		count( pm, TestClassTiny2.class, nTCT2);
	}
	
	@Test
	public void testExtentDeletionNoHierarchyNoFilter() {
		test(false, false);
	}
	
	
	@Test
	public void testExtentDeletionHierarchyNoFilter() {
		test(true, false);
	}
	
	@Test
	public void testExtentDeletionNoHierarchyFilter() {
		test(false, true);
	}
	
	
	@Test
	public void testExtentDeletionHierarchyFilter() {
		test(true, true);
	}
	
	private void test(boolean hierarchy, boolean filter) {
		int nTC2 = hierarchy ? 0 : 1;
		
		PersistenceManager pm = TestTools.openPM();
		pm.setIgnoreCache(false);
		pm.currentTransaction().begin();

		//create
		pm.makePersistent(new TestClass());
		pm.makePersistent(new TestClassTiny());
		pm.makePersistent(new TestClassTiny2());
		checkCount(pm, 1, 1, 1);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		checkCount(pm, 1, 1, 1);
		//delete
		Extent<?> ext = pm.getExtent(TestClassTiny.class, hierarchy);
		if (filter) {
			pm.newQuery(ext, "_int < 1000000").deletePersistentAll();
		} else {
			pm.newQuery(ext).deletePersistentAll();
		}
		checkCount(pm, 1, 0, nTC2);

		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();
		//check successful rollback
		checkCount(pm, 1, 1, 1);
		//delete again
		ext = pm.getExtent(TestClassTiny.class, hierarchy);
		if (filter) {
			pm.newQuery(ext, "_int < 1000000").deletePersistentAll();
		} else {
			pm.newQuery(ext).deletePersistentAll();
		}
		checkCount(pm, 1, 0, nTC2);

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		checkCount(pm, 1, 0, nTC2);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
    /**
     * Test batched deletion of objects while iterating over an extent.
     * This failed in one test program for 100.000 objects, in which case
     * it read garbage (invalid position) after ~50.000 objects.
     */
    @Test
    public void testExtentDeletionBatched() {
    	//TODO
    	System.err.println("COW on index-level has been disabled."); 
    	System.err.println("Reenabled test once multi-session is available?"); 
//        int N = 100000;
//        PersistenceManager pm = TestTools.openPM();
//        //pm.setIgnoreCache(false);
//        pm.currentTransaction().begin();
//
//        for (int i = 0; i < N; i++) {
//            TestClass tc = new TestClass();
//            tc.setInt(i);
//            pm.makePersistent(tc);
//            if (i%5000 == 0) {
//                pm.currentTransaction().commit();
//                pm.currentTransaction().begin();
//            }
//        }
//
//        pm.currentTransaction().commit();
//        TestTools.closePM();
//        
//        
//        //deletion
//        pm = TestTools.openPM();
//        pm.currentTransaction().begin();
//        int i = 0;
//        Extent<TestClass> extent = pm.getExtent(TestClass.class, false);
//        Iterator<TestClass> it = extent.iterator();
//        Object oid = null;
//        while (it.hasNext()) {
//            try {
//                TestClass tc = it.next();
//                oid = pm.getObjectId(tc);
//                pm.deletePersistent(tc);
//                
//                //re-use emptied pages
//                pm.makePersistent(new TestClassTiny2());
//                pm.makePersistent(new TestClassTiny2());
//                
//               if (++i%100 == 0) {
//                    pm.currentTransaction().commit();
//                    pm.currentTransaction().begin();
//                }
//                
//                //save some time: Usually it breaks before 900
//                if (i>=1000) break;
//            } catch (RuntimeException e) {
//                System.out.println("i="+i);
//                System.out.println(" oid=" + oid);
//                throw e;
//            }
//        }
//        extent.closeAll();
//        pm.currentTransaction().commit();
//        TestTools.closePM();
    }

    /**
     * Test batched deletion of objects while iterating over an extent.
     * This failed in one test program for 100.000 objects, in which case
     * it read garbage (invalid position) after ~50.000 objects.
     * 
     * Same as above, but w/o overwriting old pages.
     */
    @Test
    public void testExtentDeletionBatched2() {
    	//TODO
    	System.err.println("COW on index-level has been disabled."); 
    	System.err.println("Reenabled test once multi-session is available?"); 
//        int N = 100000;
//        PersistenceManager pm = TestTools.openPM();
//        //pm.setIgnoreCache(false);
//        pm.currentTransaction().begin();
//
//        for (int i = 0; i < N; i++) {
//            TestClass tc = new TestClass();
//            tc.setInt(i);
//            pm.makePersistent(tc);
//            if (i%5000 == 0) {
//                pm.currentTransaction().commit();
//                pm.currentTransaction().begin();
//            }
//        }
//
//        pm.currentTransaction().commit();
//        TestTools.closePM();
//        
//        
//        //deletion
//        pm = TestTools.openPM();
//        pm.currentTransaction().begin();
//        int i = 0;
//        Extent<TestClass> extent = pm.getExtent(TestClass.class, false);
//        Iterator<TestClass> it = extent.iterator();
//        Object oid = null;
//        while (it.hasNext()) {
//            try {
//                TestClass tc = it.next();
//                oid = pm.getObjectId(tc);
//                pm.deletePersistent(tc);
//                if (++i%20 == 0) {
//                    pm.currentTransaction().commit();
//                    pm.currentTransaction().begin();
//                }
//                
//                //save some time: Usually it breaks before 900
//                if (i>=1000) break;
//            } catch (RuntimeException e) {
//                System.out.println("i="+i);
//                System.out.println(" oid=" + oid);
//                throw e;
//            }
//        }
//        extent.closeAll();
//        pm.currentTransaction().commit();
//        TestTools.closePM();
    }
}

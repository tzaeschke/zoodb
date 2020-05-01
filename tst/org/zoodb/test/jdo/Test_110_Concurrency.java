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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.api.DBLargeVector;
import org.zoodb.test.api.TestSerializer;
import org.zoodb.test.api.TestSuper;
import org.zoodb.test.testutil.TestTools;

public class Test_110_Concurrency {


    @Before
    public void before() {
        // nothing
    }


    /**
     * Run after each test.
     */
    @After
    public void after() {
        TestTools.closePM();
    }

    
    @BeforeClass
    public static void beforeClass() {
        TestTools.createDb();
        TestTools.defineSchema(TestSerializer.class, TestSuper.class, DBLargeVector.class);
    }
    

    @AfterClass
    public static void afterClass() {
        TestTools.removeDb();
    }

//    private static class Reader extends Thread {
//        
//        private final PersistenceManager pm;
//        private final int N;
//        private int n = 0;
//        
//        private Reader(PersistenceManager pm, int n) {
//            this.pm = pm;
//            this.N = n;
//        }
//        
//        @SuppressWarnings("unchecked")
//        @Override
//        public void run() {
//            Extent<TestSuper> ext = pm.getExtent(TestSuper.class);
//            for (TestSuper t: ext) {
//                assertTrue(t.getId() >= 0 && t.getId() < N);
//                assertTrue(t.getData()[0] >= 0 && t.getData()[0] < N);
//                TestSuper t2 = (TestSuper) pm.getObjectById( JDOHelper.getObjectId(t) );
//                assertEquals(t.getId(), t2.getId());
//                n++;
//            }
//            Collection<TestSuper> col = 
//                (Collection<TestSuper>) pm.newQuery(TestSuper.class).execute();
//            for (TestSuper t: col) {
//                assertTrue(t.getId() >= 0 && t.getId() < N);
//                assertTrue(t.getData()[0] >= 0 && t.getData()[0] < N);
//                TestSuper t2 = (TestSuper) JDOHelper.getObjectId( JDOHelper.getObjectId(t) );
//                assertEquals(t.getId(), t2.getId());
//                n++;
//            }
//        }
//    }
    
    
    /**
     * Test serialisation. 
     */
    @Test
    public void testParallelRead() {
        System.err.println("Re-enable concurrency test later.");
        if (true) return;
        
        
//        final int N = 10000;
//        final int T = 10;
//        
//        PersistenceManager pm = TestTools.openPM();
//        pm.currentTransaction().begin();
//        for (int i = 0; i < N; i++) {
//            TestSuper o = new TestSuper(i, i, new long[]{i});
//            pm.makePersistent(o);
//        }
//        
//        pm.currentTransaction().commit();
//        TestTools.closePM();
//
//        pm = TestTools.openPM();
//        pm.currentTransaction().begin();
//        Reader[] readers = new Reader[T];
//        for (int i = 0; i < T; i++) {
//            readers[i] = new Reader(pm, N);
//        }
//        
//        for (Reader reader: readers) {
//            reader.start();
//        }
//        
//        for (Reader reader: readers) {
//            try {
//                reader.join();
//                assertEquals(10000 * 2, reader.n);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        }
//        
//        pm.currentTransaction().rollback();
//        TestTools.closePM();
    }

}

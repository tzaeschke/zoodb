/*
 * Copyright 2009-2012 Tilmann Zäschke. All rights reserved.
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

import java.util.Collection;

import javax.jdo.Extent;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.api.TestSerializer;
import org.zoodb.test.api.TestSuper;
import org.zoodb.test.util.TestTools;

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
        TestTools.defineSchema(TestSerializer.class, TestSuper.class);
    }
    

    @AfterClass
    public static void afterClass() {
        TestTools.removeDb();
    }

    private static class Reader extends Thread {
        
        private final PersistenceManager pm;
        private final int N;
        private int n = 0;
        
        private Reader(PersistenceManager pm, int n) {
            this.pm = pm;
            this.N = n;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void run() {
            Extent<TestSuper> ext = pm.getExtent(TestSuper.class);
            for (TestSuper t: ext) {
                assertTrue(t.getId() >= 0 && t.getId() < N);
                assertTrue(t.getData()[0] >= 0 && t.getData()[0] < N);
                TestSuper t2 = (TestSuper) pm.getObjectById( JDOHelper.getObjectId(t) );
                assertEquals(t.getId(), t2.getId());
                n++;
            }
            Collection<TestSuper> col = 
                (Collection<TestSuper>) pm.newQuery(TestSuper.class).execute();
            for (TestSuper t: col) {
                assertTrue(t.getId() >= 0 && t.getId() < N);
                assertTrue(t.getData()[0] >= 0 && t.getData()[0] < N);
                TestSuper t2 = (TestSuper) JDOHelper.getObjectId( JDOHelper.getObjectId(t) );
                assertEquals(t.getId(), t2.getId());
                n++;
            }
        }
    }
    
    
    /**
     * Test serialisation. 
     */
    @Test
    public void testParallelRead() {
        System.err.println("Re-enable concurrency test later.");
        if (true) return;
        
        
        final int N = 10000;
        final int T = 10;
        
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        for (int i = 0; i < N; i++) {
            TestSuper o = new TestSuper(i, i, new long[]{i});
            pm.makePersistent(o);
        }
        
        pm.currentTransaction().commit();
        TestTools.closePM();

        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        Reader[] readers = new Reader[T];
        for (int i = 0; i < T; i++) {
            readers[i] = new Reader(pm, N);
        }
        
        for (Reader reader: readers) {
            reader.start();
        }
        
        for (Reader reader: readers) {
            try {
                reader.join();
                assertEquals(10000 * 2, reader.n);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        
        pm.currentTransaction().rollback();
        TestTools.closePM();
    }

}

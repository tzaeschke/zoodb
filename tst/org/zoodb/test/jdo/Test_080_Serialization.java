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

import java.util.Collection;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.api.DBLargeVector;
import org.zoodb.test.api.TestSerializer;
import org.zoodb.test.api.TestSuper;
import org.zoodb.test.testutil.TestTools;
import org.zoodb.tools.ZooConfig;

import static org.junit.Assert.*;

public class Test_080_Serialization {


    @Before
    public void before() {
        TestSerializer.resetStatic();
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

    
    /**
     * Test serialisation. 
     */
    @Test
    public void testSerialization() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        TestSerializer ts1 = new TestSerializer();
        ts1.init();
        ts1.check(true);
        pm.makePersistent(ts1);
        Object oid = pm.getObjectId(ts1);
        pm.currentTransaction().commit();
        TestTools.closePM();

        TestSerializer.resetStatic();
        
        //check target
        pm = TestTools.openPM();
        pm.currentTransaction().begin();
                
        //Check for content in target
        TestSerializer ts2 = (TestSerializer) pm.getObjectById(oid, true);
        ts2.check(false);
        pm.currentTransaction().rollback();
        TestTools.closePM();

        TestSerializer.resetStatic();

        //Now try the same thing again, this time with an existing object.
        pm = TestTools.openPM();
        pm.currentTransaction().begin();

        TestSerializer ts3 = (TestSerializer) pm.getObjectById(oid);
        ts3.check(false);
        //mark dirty to enforce rewrite.
        ts3.markDirtyTS();
        pm.currentTransaction().commit();
        TestTools.closePM();
            
            
        TestSerializer.resetStatic();
        //Check target
        pm = TestTools.openPM();
        pm.currentTransaction().begin();

        TestSerializer ts4 = (TestSerializer) pm.getObjectById(oid, true);
        ts4.check(false);
        pm.currentTransaction().rollback();
        TestTools.closePM();
    }

    
    /**
     * Test serialization with queries and extents.
     */
    @SuppressWarnings("unchecked")
	@Test
    public void testSerializationWithQuery() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        TestSerializer ts1 = new TestSerializer();
        ts1.init();
        ts1.check(true);
        pm.makePersistent(ts1);
        Object oid = pm.getObjectId(ts1);
        pm.currentTransaction().commit();
        TestTools.closePM();

        TestSerializer.resetStatic();
        
        //check target
        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        
        //Check for content in target
        TestSerializer ts2 = (TestSerializer) pm.getObjectById(oid, true);
        ts2.check(false);
        pm.currentTransaction().commit();
        TestTools.closePM();

        TestSerializer.resetStatic();

        //System.out.println("Testing Query 1");
        //Now try the same thing again, this time with an existing object and a query.
        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        
        TestSerializer ts3 = (TestSerializer) pm.getObjectById(oid);
        ts3.check(false);
        //drop class locks
        pm.currentTransaction().rollback();
        pm.currentTransaction().begin();
        //mark dirty to enforce re-transmission.

        ts3.markDirtyTS();
        String QUERY_SWQ = "select from " + TestSerializer.class.getName();
        Query q = pm.newQuery(QUERY_SWQ);
        Collection<TestSerializer> qr = (Collection<TestSerializer>)q.execute();
        assertTrue(qr.iterator().hasNext());
        q.close(qr);
        pm.currentTransaction().commit();
        TestTools.closePM();

        //System.out.println("Testing Query 1-2");
        TestSerializer.resetStatic();
        
        //System.out.println("Testing Query 2");
        //Check target
        pm = TestTools.openPM();
        pm.currentTransaction().begin();

        //Check for content in target
        TestSerializer ts4 = (TestSerializer) pm.getObjectById(oid, true);
        ts4.check(false);
        pm.currentTransaction().commit();
        TestTools.closePM();

        TestSerializer.resetStatic();
        
        //System.out.println("Testing Extent 1");
        //Now try the same thing again, this time with an existing object and
        //an Extent.
        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        
        TestSerializer ts5 = (TestSerializer) pm.getObjectById(oid);
        ts5.check(false);
        //mark dirty to enforce re-transmission.
        ts5.jdoZooMarkDirty();
        Extent<TestSerializer> ex = pm.getExtent(TestSerializer.class, false);
        ex.iterator();
        ex.closeAll();
        pm.currentTransaction().commit();
        TestTools.closePM();

        TestSerializer.resetStatic();
        //System.out.println("Testing Extent 2");
        //Check target
        pm = TestTools.openPM();
        pm.currentTransaction().begin();

        TestSerializer ts6 = (TestSerializer) pm.getObjectById(oid, true);
        ts6.check(false);
        pm.currentTransaction().commit();
        TestTools.closePM();
    }


    /**
     * Test long strings and arrays.
     */
    @Test
    public void testLargeObjects() {
        final int SIZE = 5 * ZooConfig.getFilePageSize();
        final int N = 100;

        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        
        TestSuper[] tsa = new TestSuper[N];
        Object[] oids = new Object[N];
        for (int t = 0; t < N; t++) {
        	TestSuper ts = new TestSuper();
        	tsa[t] = ts;
        	pm.makePersistent(ts);

        	//fill object
        	int[] ia = new int[SIZE];
        	byte[] ba = new byte[SIZE];
        	StringBuilder sb = new StringBuilder(SIZE);
        	Object[] oa = new Object[SIZE];
        	TestSuper[] ta = new TestSuper[SIZE];
        	Long[] la = new Long[SIZE];
        	for (int i = 0; i < SIZE; i++) {
        		ia[i] = i;
        		ba[i] = (byte)(i % 100);
        		sb.append(i % 10);
        		if (i % 5 == 0) {
        			oa[i] = ts;
        			ta[i] = ts;
        		} else {
        			oa[i] = null;
        			ta[i] = null;
        		}
        		la[i] = (long) i;
        	}
        	ts.setLarge(ia, ba, sb.toString(), oa, ta, la);

        	oids[t] = pm.getObjectId(ts);
            pm.currentTransaction().commit();
            pm.currentTransaction().begin();
        }
        pm.currentTransaction().commit();
        TestTools.closePM();

        //load in new transaction
        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        
        for (int t = 0; t < N; t++) {
	        TestSuper ts = (TestSuper) pm.getObjectById(oids[t]); 
	 
	        //check object
	        int[] ia = ts.getLargeInt();
	        byte[] ba = ts.getLargeByte();
	        String str = ts.getLargeStr();
	        Object[] oa = ts.getLargeObj();
	        TestSuper[] ta = ts.getLargePersObj();
	        Long[] la = ts.getLargeLongObj();
	        for (int i = 0; i < SIZE; i++) {
                assertEquals("i=" + i + "  ia[i]=" + ia[i], ia[i], i);
                assertEquals(ba[i], (byte) (i % 100));
	            assertEquals( "" + (i % 10), "" + str.charAt(i) );
	            if (i % 5 == 0) {
                    assertSame(oa[i], ts);
                    assertSame(ta[i], ts);
	            } else {
                    assertNull(oa[i]);
                    assertNull(ta[i]);
	            }
	            assertEquals( Long.valueOf(i), la[i]);
	        }
	        
	        pm.deletePersistent(ts);
        }
        pm.currentTransaction().commit();
        TestTools.closePM();
    }
}

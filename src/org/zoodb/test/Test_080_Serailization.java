package org.zoodb.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.api.DBHashtable;
import org.zoodb.jdo.api.DBVector;
import org.zoodb.jdo.internal.server.DiskAccessOneFile;
import org.zoodb.test.api.TestSerializer;
import org.zoodb.test.api.TestSuper;

public class Test_080_Serailization {


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
        TestTools.defineSchema(TestSerializer.class);
        TestTools.defineSchema(TestSuper.class);
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
        //We should be sure that the daemon is really finished here, 
        //otherwise strange things can happen. E.g. the TestSerializer instance
        //could be copied by the PROP_INITIAL_COPY, while the TestSuper 
        //instances are not copied because they were created too late for
        //the daemon (but still in the same transaction, here in the test).
        
        Object oid = null;
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        TestSerializer ts1 = new TestSerializer();
        ts1.init();
        ts1.check(true);
        pm.makePersistent(ts1);
        oid = pm.getObjectId(ts1);
        pm.currentTransaction().commit();
        TestTools.closePM();

        TestSerializer.resetStatic();
//        waitForDaemonToSettle();
        
        //check target
        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        
        //TODO remove this
        System.err.println("TEST FIXME: Remove these once transparent activation is in place!");
        pm.getExtent(TestSuper.class).iterator();
        pm.getExtent(DBVector.class).iterator();
        pm.getExtent(DBHashtable.class).iterator();
        
        //Check for content in target
        TestSerializer ts2 = (TestSerializer) pm.getObjectById(oid, true);
        ts2.check(false);
        pm.currentTransaction().rollback();
        TestTools.closePM();
//        try {
//            //Check for content in target
//            tx = DatabaseTools.createSimpleTransaction(_dbNames[1], 0,
//                    "TestSer");
//            tx.getPersistenceManager().getObjectById(oid, true);
//            fail();
//        } catch (JDOObjectNotFoundException e) {
//            //good
//        } finally {
//            if (tx != null) {
//                tx.getPersistenceManager().close();
//            }
//        }

        TestSerializer.resetStatic();

        //Now try the same thing again, this time with an existing object.
        pm = TestTools.openPM();
        pm.currentTransaction().begin();

        //TODO remove this
        System.err.println("TEST FIXME: Remove these once transparent activation is in place!");
        pm.getExtent(TestSuper.class).iterator();
        pm.getExtent(DBVector.class).iterator();
        pm.getExtent(DBHashtable.class).iterator();
        
        TestSerializer ts3 = (TestSerializer) pm.getObjectById(oid);
        ts3.check(false);
        //mark dirty to enforce re-transmission.
        ts3.markDirty();
        pm.currentTransaction().commit();
        TestTools.closePM();
            
            
        TestSerializer.resetStatic();
//        waitForDaemonToSettle();
        //Check target
        pm = TestTools.openPM();
        pm.currentTransaction().begin();

        //TODO remove this
        System.err.println("TEST FIXME: Remove these once transparent activation is in place!");
        pm.getExtent(TestSuper.class).iterator();
        pm.getExtent(DBVector.class).iterator();
        pm.getExtent(DBHashtable.class).iterator();
        
        TestSerializer ts4 = (TestSerializer) pm.getObjectById(oid, true);
        ts4.check(false);
        pm.currentTransaction().rollback();
        TestTools.closePM();
//        try {
//            //Check for content in target
//            tx = DatabaseTools.createSimpleTransaction(_dbNames[1], 0,
//                    "TestSer");
//            tx.getPersistenceManager().getObjectById(oid, true);
//            fail();
//        } catch (JDOObjectNotFoundException e) {
//            //good
//        } finally {
//            if (tx != null) {
//                tx.getPersistenceManager().close();
//            }
//        }
    }

    
    /**
     * Test serialization with queries and extents.
     */
    @Test
    public void testSerializationWithQuery() {
        System.out.println("Testing: testSerializationWithQuery()");
        
        PersistenceManager pm = TestTools.openPM();
        Object oid = null;
        pm.currentTransaction().begin();
        TestSerializer ts1 = new TestSerializer();
        ts1.init();
        ts1.check(true);
        pm.makePersistent(ts1);
        oid = pm.getObjectId(ts1);
        pm.currentTransaction().commit();
        TestTools.closePM();

        TestSerializer.resetStatic();
//        waitForDaemonToSettle();
        
        //check target
        pm = TestTools.openPM();
        pm.currentTransaction().begin();

        //TODO remove this
        System.err.println("TEST FIXME: Remove these once transparent activation is in place!");
        pm.getExtent(TestSuper.class).iterator();
        pm.getExtent(DBVector.class).iterator();
        pm.getExtent(DBHashtable.class).iterator();
        
        //Check for content in target
        TestSerializer ts2 = (TestSerializer) pm.getObjectById(oid, true);
        ts2.check(false);
        //modify the object
        ts2.modify();
        pm.currentTransaction().commit();
        TestTools.closePM();

        TestSerializer.resetStatic();
//        waitForDaemonToSettle();

        System.out.println("Testing Query 1");
        //Now try the same thing again, this time with an existing object and a query.
        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        
        //TODO remove this
        System.err.println("TEST FIXME: Remove these once transparent activation is in place!");
        pm.getExtent(TestSuper.class).iterator();
        pm.getExtent(DBVector.class).iterator();
        pm.getExtent(DBHashtable.class).iterator();
        
        TestSerializer ts3 = (TestSerializer) pm.getObjectById(oid);
        ts3.check(false);
        //drop class locks
        pm.currentTransaction().rollback();
        pm.currentTransaction().begin();
        //mark dirty to enforce re-transmission.

        //TODO remove this
        System.err.println("TEST FIXME: Remove these once transparent activation is in place!");
        pm.getExtent(TestSerializer.class).iterator();
        pm.getExtent(TestSuper.class).iterator();
        pm.getExtent(DBVector.class).iterator();
        pm.getExtent(DBHashtable.class).iterator();
        
        //TODO the following fails, because the object is not in the cache anymore
        //TODO reload it? check spec! keep hollow in cache???
        ts3.markDirty();
        String QUERY_SWQ = "select selfoid from " + TestSerializer.class.getName();
        Query q = pm.newQuery(QUERY_SWQ);
        Iterator<TestSerializer> qi = ((Collection<TestSerializer>)q.execute()).iterator();
        //new by TZ:
        assertTrue(qi.hasNext());
        //qi.close(); //TODO
        pm.currentTransaction().commit();
        TestTools.closePM();

        System.out.println("Testing Query 1-2");
        TestSerializer.resetStatic();
        //waitForDaemonToSettle();
        
        System.out.println("Testing Query 2");
        //Check target
        pm = TestTools.openPM();
        //Check for content in target
        TestSerializer ts4 = (TestSerializer) pm.getObjectById(oid, true);
        ts4.check(false);
        //modify the object
        ts4.modify();
        pm.currentTransaction().commit();
        TestTools.closePM();

        TestSerializer.resetStatic();
        
        System.out.println("Testing Extent 1");
        //Now try the same thing again, this time with an existing object and
        //an Extent.
        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        TestSerializer ts5 = (TestSerializer) pm.getObjectById(oid);
        ts5.check(false);
        //mark dirty to enforce re-transmission.
        ts5.markDirty();
        Extent<TestSerializer> ex = pm.getExtent(TestSerializer.class, false);
        ex.iterator();
        ex.closeAll();
        pm.currentTransaction().commit();
        TestTools.closePM();

        TestSerializer.resetStatic();
//        waitForDaemonToSettle();
        System.out.println("Testing Extent 2");
        //Check target
        pm = TestTools.openPM();
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
        final int SIZE = 5*DiskAccessOneFile.PAGE_SIZE;
        final int N = 100;
        System.err.println("Test large objects!!");
        
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
        		la[i] = Long.valueOf(i);
        	}
        	ts.setLarge(ia, ba, sb.toString(), oa, ta, la);

        	oids[t] = pm.getObjectId(ts);
        	//TODO
            System.err.println("Check why OGT increases constantly with commit inside loop!");
//            pm.currentTransaction().commit();
//            pm.currentTransaction().begin();
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
	            assertTrue( ia[i] == i);
	            assertTrue( ba[i] == (byte)(i % 100) );
	            assertEquals( "" + (i % 10), "" + str.charAt(i) );
	            if (i % 5 == 0) {
	                assertTrue(oa[i] == ts);
	                assertTrue(ta[i] == ts);
	            } else {
	                assertTrue( oa[i] == null );
	                assertTrue( ta[i] == null );
	            }
	            assertEquals( Long.valueOf(i), la[i]);
	        }
	        
	        pm.deletePersistent(ts);
        }
        pm.currentTransaction().commit();
        TestTools.closePM();
    }
}

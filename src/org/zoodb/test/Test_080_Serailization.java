package org.zoodb.test;

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
import org.zoodb.test.api.TestSerializer;

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
        TestSerializer ts3 = (TestSerializer) pm.getObjectById(oid);
        ts3.check(false);
        //drop class locks
        pm.currentTransaction().rollback();
        pm.currentTransaction().begin();
        //mark dirty to enforce re-transmission.
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

    
    @Test
    public void testLargeObjects() {
        System.err.println("Test large objects!!");
        //test long strings and arrays
    }
}

package org.zoodb.test.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.WeakHashMap;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.naming.ConfigurationException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.TestTools;

/**
 * Test harness for TransientField.
 *
 * @author  Tilmann Zaeschke
 */
public final class TransientFieldTest {

	private static final String DB_NAME = "TestDb";
	
    /**
     * Run before each test.
     */
    @Before
    public void before() {
        //Nothing to do
    }

    /**
     * Run after each test.
     */
    @After
    public void after() {
        //Nothing to do
    }

    /**
     * Initialise test suite, called only once before test suite is run.
     * @throws ConfigurationException
     * @throws StoreException
     */
    @BeforeClass
    public static void beforeClass() {
		TestTools.createDb(DB_NAME);
		TestTools.defineSchema(DB_NAME, TestTransient.class);
    }
    
    /**
     * Tear down test suite, called only once before test suite is run.
     * @throws StoreException
     */
    @AfterClass
    public static void afterClass() {
		TestTools.removeDb(DB_NAME);
    }

    /**
     * Test initialisation of transient variables.
     */
    @Test
    public void testInitialization() {
    	System.out.println("Test 1");
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

        TestTransient tt1 = new TestTransient();

    	//get defaults
    	assertEquals(true, tt1.getTb1());
    	assertEquals(null, tt1.getTb2());
    	assertEquals(Boolean.TRUE, tt1.getTb3());
    	try {
    		tt1.getTb2F();
    		fail();
    	} catch (NullPointerException e) {
    		//OK
    	}
    	assertEquals(null, tt1.getTo1());
    	assertNotSame(null, tt1.getTo2());

    	//set 
    	tt1.setTb1(true);
    	tt1.setTb2(Boolean.TRUE);
    	tt1.setTb3(null);
    	//overwrite
    	tt1.setTb1F(Boolean.FALSE);
    	tt1.setTb2F(false);
    	tt1.setTb3F(true);
    	tt1.setTo1("cc");
    	tt1.setTo2(null);
    	
    	//get
    	assertEquals(false, tt1.getTb1());
    	assertEquals(Boolean.FALSE, tt1.getTb2());
    	assertEquals(Boolean.TRUE, tt1.getTb3());
    	assertEquals("cc", tt1.getTo1());
    	assertEquals(null, tt1.getTo2());
    	
    	TestTools.closePM(pm);
    }
    
    /**
     * Test setting of transient variables for multiple objects.
     */
    @Test
    public void testUniquity() {
        System.out.println("Test 2");
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

        TestTransient tt1 = new TestTransient();
        TestTransient tt2 = new TestTransient();
        tt1.setTb1(true);
        tt1.setTb2(Boolean.TRUE);
        tt1.setTo1("Hello1");
        tt1.setTo2("Hello1");
        tt2.setTb1(false);
        tt2.setTb2(Boolean.FALSE);
        tt2.setTo1("Hello2");
        tt2.setTo2(null);
        assertEquals(true, tt1.getTb1());
        assertEquals(Boolean.TRUE, tt1.getTb2());
        assertEquals("Hello1", tt1.getTo1());
        assertEquals("Hello1", tt1.getTo2());
        assertEquals(false, tt2.getTb1());
        assertEquals(Boolean.FALSE, tt2.getTb2());
        assertEquals("Hello2", tt2.getTo1());
        assertEquals(null, tt2.getTo2());
        
        TestTools.closePM(pm);
    }
    
    /**
     * Test re-associating of transient variables after reloading of parents.
     */
    @Test
    public void testBecomePersistent() {
    	System.out.println("Test 3");
		PersistenceManager pm = null;
        
        try {
    		pm = TestTools.openPM();
    		pm.currentTransaction().begin();

            TestTransient tt1 = new TestTransient();
            TestTransient tt2 = new TestTransient();
            TestTransient tt3 = new TestTransient();
            tt1.setTb1(true);
            tt1.setTb2(Boolean.TRUE);
            tt1.setTo1("Hello1");
            tt1.setTo2(tt2);
            tt2.setTb1(false);
            tt2.setTb2(Boolean.FALSE);
            tt2.setTo1(tt3);
            tt2.setTo2(null);

            pm.makePersistent(tt1);
            //Object TT1ID = pm.getObjectId(tt1);
            pm.makePersistent(tt2);

            assertEquals(true, tt1.getTb1());
            assertEquals(Boolean.TRUE, tt1.getTb2());
            assertEquals("Hello1", tt1.getTo1());
            assertEquals(tt2, tt1.getTo2());
            assertEquals(false, tt2.getTb1());
            assertEquals(Boolean.FALSE, tt2.getTb2());
            assertEquals(tt3, tt2.getTo1());
            assertEquals(null, tt2.getTo2());

            pm.currentTransaction().commit();
            pm.currentTransaction().begin();

            assertEquals(true, tt1.getTb1());
            assertEquals(Boolean.TRUE, tt1.getTb2());
            assertEquals("Hello1", tt1.getTo1());
            assertEquals(tt2, tt1.getTo2());
            assertEquals(false, tt2.getTb1());
            assertEquals(Boolean.FALSE, tt2.getTb2());
            assertEquals(tt3, tt2.getTo1());
            assertEquals(null, tt2.getTo2());

            pm.currentTransaction().rollback();
            pm.currentTransaction().begin();

            assertEquals(true, tt1.getTb1());
            assertEquals(Boolean.TRUE, tt1.getTb2());
            assertEquals("Hello1", tt1.getTo1());
            assertEquals(tt2, tt1.getTo2());
            assertEquals(false, tt2.getTb1());
            assertEquals(Boolean.FALSE, tt2.getTb2());
            assertEquals(tt3, tt2.getTo1());
            assertEquals(null, tt2.getTo2());
        } finally {
            if (pm != null) {
            	TestTools.closePM(pm);
            }
        }
    }
    
    /**
     * Test re-associating of transient variables after reloading of parents.
     */
    @Test
    public void testReload() {
    	System.out.println("Test 4");
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
        
    	TestTransient tt1 = new TestTransient();
    	TestTransient tt2 = new TestTransient();
    	TestTransient tt3 = new TestTransient();
    	tt3.setTo2("TT3");
//        pm.addNamedRoot("TTreload1", tt1);
//        pm.addNamedRoot("TTreload2", tt2);
        pm.makePersistent(tt1);
        Object TTreload1ID = pm.getObjectId(tt1);
        pm.makePersistent(tt2);
        Object TTreload2ID = pm.getObjectId(tt2);
    	tt1.setTb1(true);
    	tt1.setTb2(Boolean.TRUE);
    	tt1.setTo1("Hello");
    	tt1.setTo2(tt2);
    	assertNotNull(tt1.getTo2());
    	tt2.setTo1(null);
    	tt2.setTo2(tt3);
    	assertEquals(false, JDOHelper.isPersistent(tt3));
    	pm.currentTransaction().commit();
    	pm.currentTransaction().begin();
    	Object[] oa = new Object[]{tt1, tt2};
    	tt1 = null;
    	tt2 = null;
    	tt3 = null;
    	pm.evictAll(oa);
    	oa = null;
    	pm.currentTransaction().commit();
    	TestTools.closePM(pm);
        
    	System.gc();
        
        
        //Start next transaction ***********************
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
        TestTransient tt1b = (TestTransient) pm.getObjectById(TTreload1ID);
        TestTransient tt2b = (TestTransient) pm.getObjectById(TTreload2ID);
    	assertEquals(true, tt1b.getTb1());
    	assertEquals(null, tt1b.getTb2());
    	assertEquals(null, tt1b.getTo1());
    	assertEquals(null, tt2b.getTo1());
    	assertEquals("fdfd", tt1b.getTo2());
    	assertEquals("fdfd", tt2b.getTo2());
        
        pm.currentTransaction().rollback();
        TestTools.closePM();
    }

    /**
     * Test unregistering TransientFields.
     */
    @Test
    public void testDeRegister() {
    	System.out.println("Test 5");
		PersistenceManager pm = null;
        try {
    		pm = TestTools.openPM();
    		pm.currentTransaction().begin();

            TestTransient tt1 = new TestTransient();
            TestTransient tt2 = new TestTransient();
            //transient
            TestTransient tt3 = new TestTransient();
            tt1.setTb1(true);
            tt1.setTb2(Boolean.TRUE);
            tt1.setTo1("Hello1");
            tt1.setTo2(tt2);
            tt2.setTb1(false);
            tt2.setTb2(Boolean.FALSE);
            tt2.setTo1(tt3);
            tt2.setTo2(null);
            tt3.setTb1(false);
            tt3.setTb2(Boolean.FALSE);
            tt3.setTo1("Hello3");
            tt3.setTo2(null);

            pm.makePersistent(tt1);
            pm.makePersistent(tt2);

            pm.currentTransaction().commit();
            pm.currentTransaction().begin();

            tt1.deregister();
            //tt2 is kept!
            tt3.deregister();

            //should be reset
            assertEquals(true, tt1.getTb1());
            assertEquals(null, tt1.getTb2());
            assertEquals(Boolean.TRUE, tt1.getTb3());
            assertEquals(null, tt1.getTo1());
            assertEquals("fdfd", tt1.getTo2());

            //should be the same
            assertEquals(false, tt2.getTb1());
            assertEquals(Boolean.FALSE, tt2.getTb2());
            assertEquals(Boolean.TRUE, tt2.getTb3());
            assertEquals(tt3, tt2.getTo1());
            assertEquals(null, tt2.getTo2());

            //should be reset
            assertEquals(true, tt3.getTb1());
            assertEquals(null, tt3.getTb2());
            assertEquals(Boolean.TRUE, tt3.getTb3());
            assertEquals(null, tt3.getTo1());
            assertEquals("fdfd", tt3.getTo2());
        } finally {
            if (pm != null) {
                TestTools.closePM();
            }
        }
    }
    
    
    /**
     * Test re-associating of transient variables after reloading of parents.
     */
    @Test
    public void testOutsideStore() {
    	System.out.println("Test 6");
        
    	//Test before Store
    	TestTransient tt1 = new TestTransient();
    	TestTransient tt2 = new TestTransient();
    	TestTransient tt3 = new TestTransient();
    	tt3.setTo2("TT6");
    	tt1.setTb1(true);
    	tt1.setTb2(Boolean.TRUE);
    	tt1.setTo1("Hello");
    	tt1.setTo2(tt2);
    	assertNotNull(tt1.getTo2());
    	tt2.setTo1(null);
    	tt2.setTo2(tt3);

    	//Use in Store
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
    	//Object TT6out = pm.addNamedRoot("TT6out", tt1);
		pm.makePersistent(tt1);
		Object TT6ouID = pm.getObjectId(tt1);
    	pm.makePersistent(tt2);
    	pm.currentTransaction().commit();
    	pm.currentTransaction().begin();
//    	Object[] oa = new Object[]{tt1, tt2};
//    	tt1 = null;
//    	tt2 = null;
//    	tt3 = null;
 //   	store.evictAll(oa);
//    	oa = null;
    	pm.currentTransaction().commit();
    	TestTools.closePM(pm);
        
    	System.gc();
        

    	//Test after Store
    	assertEquals(true, tt1.getTb1());
    	assertEquals(Boolean.TRUE, tt1.getTb2());
    	assertEquals("Hello", tt1.getTo1());
    	assertNotNull(tt1.getTo2());
    	assertEquals(null, tt2.getTo1());
    	assertNotNull(tt2.getTo2());  //returns tt3
    	assertEquals("TT6", ((TestTransient)tt2.getTo2()).getTo2()); 


    	//Cleanup
    	tt1 = null;
    	tt2 = null;
    	tt3 = null;
    	System.gc();
    	
    	
        //Start next transaction ***********************
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
        TestTransient tt1b = (TestTransient) pm.getObjectById(TT6ouID);
    	assertEquals(true, tt1b.getTb1());
    	assertEquals(null, tt1b.getTb2());
    	assertEquals(null, tt1b.getTo1());
    	assertEquals("fdfd", tt1b.getTo2());
        
    	pm.currentTransaction().rollback();
        TestTools.closePM(pm);
    }

    /**
     * This test is supposed to reproduce a problem encountered in the PHS
     * server process. It happened that a key turned up both the _tMap and the 
     * _pMap. 
     * The problem could no be reproduced, see also TransientField.java.
     */
//    @Test
//    public void testBecomeTransient() {
////      Test has been commented out, because it never reproduced the problem.
//        System.out.println("Test 7");
//        ObjectStore store = StoreFactory.create().createStore(DB_NAME_PROP);
//        store.begin();
//
//        TestTransient tt1 = new TestTransient();
//        TestTransient tt2 = new TestTransient();
//        TestTransient tt3 = new TestTransient();
//        
//        store.addNamedRoot("TT7", tt1);
//        store.makePersistent(tt2);
//
////        store.commit();
//        
//        TestTransient tt11 = tt1.clone();
//        store.makePersistent(tt11);
//
//        tt1.setTb1(true);
//        tt1.setTb2(Boolean.TRUE);
//        tt1.setTo1("Hello1");
//        tt1.setTo2(tt2);
//        tt2.setTb1(false);
//        tt2.setTb2(Boolean.FALSE);
//        tt2.setTo1(tt3);
//        tt2.setTo2(null);
//
//        System.out.println("111  " + JDOHelper.getObjectIdAsString(tt1));        
//        System.out.println("1111 " + JDOHelper.getObjectIdAsString(tt11));        
//        System.out.println("112  " + JDOHelper.getObjectIdAsString(tt2));        
////        assertEquals(true, tt1.getTb1());
////        assertEquals(Boolean.TRUE, tt1.getTb2());
////        assertEquals("Hello1", tt1.getTo1());
////        assertEquals(tt2, tt1.getTo2());
////        assertEquals(false, tt2.getTb1());
////        assertEquals(Boolean.FALSE, tt2.getTb2());
////        assertEquals(tt3, tt2.getTo1());
////        assertEquals(null, tt2.getTo2());
//
//        store.abort();
//
//        System.out.println("211  " + JDOHelper.getObjectIdAsString(tt1));        
//        System.out.println("2111 " + JDOHelper.getObjectIdAsString(tt11));        
//        System.out.println("212  " + JDOHelper.getObjectIdAsString(tt2));        
//        tt1.setTb1(true);
//        tt1.setTb2(Boolean.TRUE);
//        tt1.setTo1("Hello1");
//        tt1.setTo2(tt2);
//        tt11.setTb1(true);
//        tt11.setTb2(Boolean.TRUE);
//        tt11.setTo1("Hello1");
//        tt11.setTo2(tt2);
//        tt2.setTb1(false);
//        tt2.setTb2(Boolean.FALSE);
//        tt2.setTo1(tt3);
//        tt2.setTo2(null);
//        
////        assertEquals(true, tt1.getTb1());
////        assertEquals(Boolean.TRUE, tt1.getTb2());
////        assertEquals("Hello1", tt1.getTo1());
////        assertEquals(tt2, tt1.getTo2());
////        assertEquals(false, tt2.getTb1());
////        assertEquals(Boolean.FALSE, tt2.getTb2());
////        assertEquals(tt3, tt2.getTo1());
////        assertEquals(null, tt2.getTo2());
//
//        store.addNamedRoot("TT7", tt1);
//        store.makePersistent(tt2);
//        tt1.setTb1(true);
//        tt1.setTb2(Boolean.TRUE);
//        tt1.setTo1("Hello1");
//        tt1.setTo2(tt2);
//        tt2.setTb1(false);
//        tt2.setTb2(Boolean.FALSE);
//        tt2.setTo1(tt3);
//        tt2.setTo2(null);
//        store.commit();
//        System.out.println("211  " + JDOHelper.getObjectIdAsString(tt1));        
//        System.out.println("212  " + JDOHelper.getObjectIdAsString(tt2));        
//
//        tt1.setTb1(true);
//        tt1.setTb2(Boolean.TRUE);
//        tt1.setTo1("Hello1");
//        tt1.setTo2(tt2);
//        tt2.setTb1(false);
//        tt2.setTb2(Boolean.FALSE);
//        tt2.setTo1(tt3);
//        tt2.setTo2(null);
//        
//        assertEquals(true, tt1.getTb1());
//        assertEquals(Boolean.TRUE, tt1.getTb2());
//        assertEquals("Hello1", tt1.getTo1());
//        assertEquals(tt2, tt1.getTo2());
//        assertEquals(false, tt2.getTb1());
//        assertEquals(Boolean.FALSE, tt2.getTb2());
//        assertEquals(tt3, tt2.getTo1());
//        assertEquals(null, tt2.getTo2());
//
//        store.exit();
//    }
//    
    /**
     * Test garbage collection of owners and transient values.
     * The owners should always be collectible.
     * The values should be collectible if the owners are.
     * @throws InterruptedException 
     */
    @Test
    public void testGC() throws InterruptedException {
        System.out.println("Test 8");
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

        Map<Object, Object> wKeys = new WeakHashMap<Object, Object>();
        Map<Object, Object> wValues = new WeakHashMap<Object, Object>();
        
        //Transient
        final int MAX_I = 10;
        for (int i = 0; i < MAX_I; i++ ) {
            TestTransient tt1 = new TestTransient();
            TestTransient tt2 = new TestTransient();
            Object o3 = new Object();
            tt1.setTo1(tt2);
            tt1.setTo2(o3);
            wKeys.put(tt1, null);
            wValues.put(tt2, null);
            wValues.put(o3, null);
//            System.out.println("X " + TestTransient.getTfTo1().size(null) + " / " +
//                    TestTransient.getTfTo2().size(null));
        }
        
        //Wait for gc
        int w = 0;
        do {
            if (w++ >= 100) {
                System.out.println(TestTransient.getTfTo1().size(null));
                System.out.println(TestTransient.getTfTo2().size(null));
                fail("Failed(" + w + "): " + wKeys.size() + "/" + 
                        wValues.size() + "\n" + wKeys + " === " + wValues);
            }
            Thread.sleep(100);
            System.gc();
            
        } while (wKeys.size() + wValues.size() >= MAX_I);
        
        //TODO remove?
        wKeys.clear();
        wValues.clear();
        
        System.out.println("*** CLEARED ***");
        
        //Persistent
        for (int i = 0; i < MAX_I; i++ ) {
            TestTransient tt = new TestTransient();
            pm.makePersistent(tt);
            TestTransient tt1 = new TestTransient();
            pm.makePersistent(tt1);
            TestTransient tt2 = new TestTransient();
            Object o3 = new Object();
            tt.setTo1(tt1);
            tt.setTo1(tt2);
            tt.setTo2(o3);
            wKeys.put(tt, null);
            wValues.put(tt1, null);
            wValues.put(tt2, null);
            wValues.put(o3, null);
        }
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();
        
        //Wait for gc, which should not delete anything
        for (int i = 0; i < 20; i++) {
            if (wKeys.size() + wValues.size() < MAX_I) {
                fail("Failed: " + wKeys.size() + "/" + wValues.size() + "\n");
                        //+ wKeys + " === " + wValues);
            }
            Thread.sleep(100);
            System.gc();
        }
        
        TestTools.closePM(pm);
        
        //Wait for gc
        w = 0;
        do {
            if (w++ >= 100) {
                fail("Failed: " + wKeys.size() + "/" + wValues.size() + "\n");
                        //+ wKeys + " === " + wValues);
            }
            Thread.sleep(100);
            System.gc();
            
        } while (wKeys.size() + wValues.size() >= MAX_I);
    }
    
    /**
     * Test multiple Stores.
     * @throws InterruptedException 
     */
    @Test
    public void testMultipleStores() throws InterruptedException {
        System.out.println("Test 8");
		PersistenceManager pm1 = TestTools.openPM();
		pm1.currentTransaction().begin();

        Object oidO1 = null;
        Object oidV2 = null;
       
        //Transient
        TestTransient tto1 = new TestTransient();
        TestTransient tto2 = new TestTransient();
        TestTransient ttv1 = new TestTransient();
        TestTransient ttv2 = new TestTransient();
        tto1.setTo1(ttv1);
        tto2.setTo1(ttv2);
        tto1.setTo2("tto1");
        tto2.setTo2("tto2");
        ttv1.setTo2("ttv1");
        ttv2.setTo2("ttv2");
        pm1.makePersistent(tto1);
        pm1.makePersistent(ttv2);
        oidO1 = pm1.getObjectId(tto1);
        oidV2 = pm1.getObjectId(ttv2);
        pm1.currentTransaction().commit();
        //store1.leave(); TODO JDO?
        
		PersistenceManager pm2 = TestTools.openPM();
		pm2.currentTransaction().begin();
		TestTransient tto1_2 = (TestTransient) pm2.getObjectById(oidO1);
        TestTransient ttv2_2 = (TestTransient) pm2.getObjectById(oidV2);
        assertEquals(null, tto1_2.getTo1());
        assertEquals("fdfd", tto1_2.getTo2());
        assertEquals("fdfd", ttv2_2.getTo2());
        tto1_2.setTo1(null);
        ttv2_2.setTo1(null);
        tto1_2.setTo2("tto1_2");
        ttv2_2.setTo2("ttv2_2");
        TestTools.closePM(pm2);

        // store1.join(); //TODO JDO?
        assertEquals("tto1", tto1.getTo2());
        assertEquals("tto2", tto2.getTo2());
        assertEquals("ttv1", ttv1.getTo2());
        assertEquals("ttv2", ttv2.getTo2());
        assertEquals("ttv1", ((TestTransient)tto1.getTo1()).getTo2());
        assertEquals("ttv2", ((TestTransient)tto2.getTo1()).getTo2());
        TestTools.closePM(pm1);
    }
}

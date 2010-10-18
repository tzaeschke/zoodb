package org.zoodb.test.api;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.WeakHashMap;

import javax.jdo.spi.PersistenceCapable;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test harness for TransientField.
 *
 * @author  Tilmann Zaeschke
 */
public final class TransientFieldTest {

    private static final String DB_NAME_PROP = "zoodb.test.database";
    private static String _dbName;
    
    /**
     * Run before each test.
     */
    @Before
    protected void setUp() {
        //Nothing to do
    }

    /**
     * Run after each test.
     */
    @After
    protected void tearDown() {
        //Nothing to do
    }

    /**
     * Initialise test suite, called only once before test suite is run.
     * @throws ConfigurationException
     * @throws StoreException
     */
    @BeforeClass
    public static void oneTimeSetUp() {
        _dbName = Configuration.getProperty(DB_NAME_PROP);
        DBUtil.cleanDb(_dbName);
        DBUtil.defineClassSchema(_dbName, TestTransient.class);
    }
    
    /**
     * Tear down test suite, called only once before test suite is run.
     * @throws StoreException
     */
    @AfterClass
    public static void oneTimeTearDown() {
        try {
            DatabaseTools.getCurrentTransaction().getPersistenceManager().close();
        } catch (Exception e) {
            // ignore
        }
        DBUtil.dropClassSchema(_dbName, TestTransient.class);
    }

    /**
     * Test initialisation of transient variables.
     */
    @Test
    public static void testInitialization() {
    	System.out.println("Test 1");
        ObjectStore store = StoreFactory.create().createStore(DB_NAME_PROP);
        store.begin();

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
    	
    	store.exit();
    }
    
    /**
     * Test setting of transient variables for multiple objects.
     */
    @Test
    public static void testUniquity() {
        System.out.println("Test 2");
        ObjectStore store = StoreFactory.create().createStore(DB_NAME_PROP);
        store.begin();

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
        
        store.exit();
    }
    
    /**
     * Test setting of transient variables for multiple objects.
     */
    @Test
    public static void testCleanUpVersionProxy() {
        System.out.println("Test CleanUp");
        ObjectStore store = StoreFactory.create().createStore(DB_NAME_PROP);
        store.begin();

        PersistenceManager pm = null;
        int nTemp = mapSize(VersionProxyImpl.class, "_temp", pm);
        int nHasTemp = mapSize(VersionProxyImpl.class, "_hasTemp", pm);
        
        VersionProxyImpl vp1 = new VersionProxyDummyImpl();
        pm = JDOHelper.getPersistenceManager(vp1); 
        
        vp1.createTemporaryVersion(new VersionableTest());
        assertEquals(nTemp + 1, mapSize(VersionProxyImpl.class, "_temp", pm));     
        assertEquals(nHasTemp + 1, mapSize(VersionProxyImpl.class, "_hasTemp", pm));     
        
        vp1.promoteTemporaryVersion();
        pm = JDOHelper.getPersistenceManager(vp1); 
        assertEquals(nTemp, mapSize(VersionProxyImpl.class, "_temp", pm));     
        assertEquals(nHasTemp, mapSize(VersionProxyImpl.class, "_hasTemp", pm));     

        VersionProxyImpl vp2 = new VersionProxyDummyImpl();
        vp2.createTemporaryVersion(new VersionableTest());
        pm = JDOHelper.getPersistenceManager(vp1); 
        assertEquals(nTemp + 1, mapSize(VersionProxyImpl.class, "_temp", pm));     
        assertEquals(nHasTemp + 1, mapSize(VersionProxyImpl.class, "_hasTemp", pm));     
        
        vp2.deleteTemporaryVersion();
        pm = JDOHelper.getPersistenceManager(vp1); 
        assertEquals(nTemp, mapSize(VersionProxyImpl.class, "_temp", pm));     
        assertEquals(nHasTemp, mapSize(VersionProxyImpl.class, "_hasTemp", pm));     
        
        //TODO test with make persistent
        
        
        
        store.exit();
    }
    
    @SuppressWarnings("unchecked")
    private static int mapSize(Class parent, String fieldName, 
            PersistenceManager pm) {
        try {
            Field f = parent.getDeclaredField(fieldName);
            f.setAccessible(true);
            TransientField tf = (TransientField) f.get(null);
            return tf.size(pm);
//            Method mSize = TransientField.class.getDeclaredMethod("size");
//            mSize.setAccessible(true);
//            return (Integer) mSize.invoke(tf);
        } catch (SecurityException e) {
            throw new RuntimeException(e);
//        } catch (NoSuchMethodException e) {
//            throw new RuntimeException(e);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
//        } catch (InvocationTargetException e) {
//            throw new RuntimeException(e);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Test re-associating of transient variables after reloading of parents.
     */
    @Test
    public static void testBecomePersistent() {
    	System.out.println("Test 3");
        ObjectStore store = null;
        
        try {
            store = StoreFactory.create().createStore(DB_NAME_PROP);
            store.begin();

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

            store.addNamedRoot("TT1", tt1);
            store.makePersistent(tt2);

            assertEquals(true, tt1.getTb1());
            assertEquals(Boolean.TRUE, tt1.getTb2());
            assertEquals("Hello1", tt1.getTo1());
            assertEquals(tt2, tt1.getTo2());
            assertEquals(false, tt2.getTb1());
            assertEquals(Boolean.FALSE, tt2.getTb2());
            assertEquals(tt3, tt2.getTo1());
            assertEquals(null, tt2.getTo2());

            store.commit();

            assertEquals(true, tt1.getTb1());
            assertEquals(Boolean.TRUE, tt1.getTb2());
            assertEquals("Hello1", tt1.getTo1());
            assertEquals(tt2, tt1.getTo2());
            assertEquals(false, tt2.getTb1());
            assertEquals(Boolean.FALSE, tt2.getTb2());
            assertEquals(tt3, tt2.getTo1());
            assertEquals(null, tt2.getTo2());

            store.abort();

            assertEquals(true, tt1.getTb1());
            assertEquals(Boolean.TRUE, tt1.getTb2());
            assertEquals("Hello1", tt1.getTo1());
            assertEquals(tt2, tt1.getTo2());
            assertEquals(false, tt2.getTb1());
            assertEquals(Boolean.FALSE, tt2.getTb2());
            assertEquals(tt3, tt2.getTo1());
            assertEquals(null, tt2.getTo2());
        } finally {
            if (store != null) {
                store.exit();
            }
        }
    }
    
    /**
     * Test re-associating of transient variables after reloading of parents.
     */
    @Test
    public static void testReload() {
    	System.out.println("Test 4");
        ObjectStore store = StoreFactory.create().createStore(DB_NAME_PROP);
        store.begin();
        
    	TestTransient tt1 = new TestTransient();
    	TestTransient tt2 = new TestTransient();
    	TestTransient tt3 = new TestTransient();
    	tt3.setTo2("TT3");
        store.addNamedRoot("TTreload1", tt1);
        store.addNamedRoot("TTreload2", tt2);
    	tt1.setTb1(true);
    	tt1.setTb2(Boolean.TRUE);
    	tt1.setTo1("Hello");
    	tt1.setTo2(tt2);
    	assertNotNull(tt1.getTo2());
    	tt2.setTo1(null);
    	tt2.setTo2(tt3);
    	assertEquals(false, JDOHelper.isPersistent(tt3));
    	store.commit();
    	Object[] oa = new Object[]{tt1, tt2};
    	tt1 = null;
    	tt2 = null;
    	tt3 = null;
    	store.evictAll(oa);
    	oa = null;
    	store.commit();
    	store.exit();
        
    	System.gc();
        
        
        //Start next transaction ***********************
        store = StoreFactory.create().createStore(DB_NAME_PROP);
        store.begin();
        TestTransient tt1b = (TestTransient) store.findNamedRoot("TTreload1");
        TestTransient tt2b = (TestTransient) store.findNamedRoot("TTreload2");
    	assertEquals(true, tt1b.getTb1());
    	assertEquals(null, tt1b.getTb2());
    	assertEquals(null, tt1b.getTo1());
    	assertEquals(null, tt2b.getTo1());
    	assertEquals("fdfd", tt1b.getTo2());
    	assertEquals("fdfd", tt2b.getTo2());
        
        store.abort();
        store.exit();
    }

    /**
     * Test unregistering TransientFields.
     */
    @Test
    public static void testDeRegister() {
    	System.out.println("Test 5");
        ObjectStore store = null;
        try {
            store = StoreFactory.create().createStore(DB_NAME_PROP);
            store.begin();

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

            store.makePersistent(tt1);
            store.makePersistent(tt2);

            store.commit();

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
            if (store != null) {
                store.exit();
            }
        }
    }
    
    
    /**
     * Test re-associating of transient variables after reloading of parents.
     */
    @Test
    public static void testOutsideStore() {
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
        ObjectStore store = StoreFactory.create().createStore(DB_NAME_PROP);
        store.begin();
    	store.addNamedRoot("TT6out", tt1);
    	store.makePersistent(tt2);
    	store.commit();
//    	Object[] oa = new Object[]{tt1, tt2};
//    	tt1 = null;
//    	tt2 = null;
//    	tt3 = null;
 //   	store.evictAll(oa);
//    	oa = null;
    	store.commit();
    	store.exit();
        
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
        store = StoreFactory.create().createStore(DB_NAME_PROP);
        store.begin();
        TestTransient tt1b = (TestTransient) store.findNamedRoot("TT6out");
    	assertEquals(true, tt1b.getTb1());
    	assertEquals(null, tt1b.getTb2());
    	assertEquals(null, tt1b.getTo1());
    	assertEquals("fdfd", tt1b.getTo2());
        
        store.abort();
        store.exit();
    }

    /**
     * This test is supposed to reproduce a problem encountered in the PHS
     * server process. It happened that a key turned up both the _tMap and the 
     * _pMap. 
     * The problem could no be reproduced, see also TransientField.java.
     */
//    @Test
//    public static void testBecomeTransient() {
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
    public static void testGC() throws InterruptedException {
        System.out.println("Test 8");
        ObjectStore store = StoreFactory.create().createStore(DB_NAME_PROP);
        store.begin();

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
            store.makePersistent(tt);
            TestTransient tt1 = new TestTransient();
            store.makePersistent(tt1);
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
        store.commit();
        
        //Wait for gc, which should not delete anything
        for (int i = 0; i < 20; i++) {
            if (wKeys.size() + wValues.size() < MAX_I) {
                fail("Failed: " + wKeys.size() + "/" + wValues.size() + "\n");
                        //+ wKeys + " === " + wValues);
            }
            Thread.sleep(100);
            System.gc();
        }
        
        store.exit();
        
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
    public static void testMultipleStores() throws InterruptedException {
        System.out.println("Test 8");
        ObjectStore store1 = StoreFactory.create().createStore(DB_NAME_PROP);
        store1.begin();

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
        store1.makePersistent(tto1);
        store1.makePersistent(ttv2);
        oidO1 = store1.getObjectId(tto1);
        oidV2 = store1.getObjectId(ttv2);
        store1.commit();
        store1.leave();
        
        ObjectStore store2 = StoreFactory.create().createStore(DB_NAME_PROP);
        store2.begin();
        TestTransient tto1_2 = (TestTransient) store2.getObjectById(oidO1);
        TestTransient ttv2_2 = (TestTransient) store2.getObjectById(oidV2);
        assertEquals(null, tto1_2.getTo1());
        assertEquals("fdfd", tto1_2.getTo2());
        assertEquals("fdfd", ttv2_2.getTo2());
        tto1_2.setTo1(null);
        ttv2_2.setTo1(null);
        tto1_2.setTo2("tto1_2");
        ttv2_2.setTo2("ttv2_2");
        store2.exit();

        store1.join();
        assertEquals("tto1", tto1.getTo2());
        assertEquals("tto2", tto2.getTo2());
        assertEquals("ttv1", ttv1.getTo2());
        assertEquals("ttv2", ttv2.getTo2());
        assertEquals("ttv1", ((TestTransient)tto1.getTo1()).getTo2());
        assertEquals("ttv2", ((TestTransient)tto2.getTo1()).getTo2());
        store1.exit();
    }
}

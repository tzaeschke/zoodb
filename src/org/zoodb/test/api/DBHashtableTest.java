package org.zoodb.test.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.api.DBHashtable;
import org.zoodb.test.TestTools;

/**
 * Test harness for DBHashtable.
 *
 * @author  Tilmann Zaeschke
 */
public final class DBHashtableTest {

    private static final String KEY1 = "1";
    private static final String KEY2 = "2";
    private static final String KEY3 = "3";
    private static final String KEY4 = "4";
    private static final String KEY5 = "5";
    private static final String KEY6 = "6";
    private static final String ELEMENT1 = "first one";
    private static final String ELEMENT2 = "second element";
    private static final String ELEMENT3 = "third element";
    private static final String ELEMENT4 = "fourth element";
    private static final String ELEMENT5 = "fifth element";
    private static final String ELEMENT6 = "sixth element";
    
    private DBHashtable<String, String> _dbHashtable;
    
    /**
     * Run before each test.
     * The setUp method tests the put method of DBHashtable.
     * @throws StoreException 
     */
    @Before
    public void before() {
        //create a DBHashtable
        _dbHashtable = new DBHashtable<String, String>();
        _dbHashtable.put(KEY1, ELEMENT1);
        _dbHashtable.put(KEY2, ELEMENT2);
        _dbHashtable.put(KEY3, ELEMENT3);
    }

    /**
     * Run after each test.
     */
    @After
    public void after() {
        _dbHashtable.clear();
    }

    @BeforeClass
    public static void beforeClass() {
    	TestTools.createDb();
    }
    
    @AfterClass
    public static void afterClass() {
    	TestTools.removeDb();
    }
    
    /**
     * Test the clear() and size() methods.
     */
    @Test
    public void testClearAndSize() {
        assertEquals("Check size 1", 3, _dbHashtable.size());
        _dbHashtable.clear();
        assertEquals("Check size 2", 0, _dbHashtable.size());
    }
    
    /**
     * Test the values method returns the correct collection.
     */
    @Test
    public void testIterator() {
    	HashSet<String> temp = new HashSet<String>();
    	temp.add(ELEMENT1);
    	temp.add(ELEMENT2);
    	temp.add(ELEMENT3);
        Iterator<String> i = _dbHashtable.values().iterator();
        while (i.hasNext()) {
        	assertTrue(temp.remove(i.next()));
        }
        assertTrue(temp.isEmpty());
        assertTrue("Check the number of remaining elements", i.hasNext() == false);
    }

    /**
     * Test the remove method removes the correct values.
     */
    @Test
    public void testRemove() {
        _dbHashtable.remove(KEY2);
        assertEquals("Check element 1", ELEMENT1, _dbHashtable.get(KEY1));
        assertEquals("Check element 2", null, _dbHashtable.get(KEY2));
        assertEquals("Check element 3", ELEMENT3, _dbHashtable.get(KEY3));
        assertTrue("Check the number of values", _dbHashtable.size()==2);
    }

    /**
     * Test the contains method returns the correct results.
     */
    @Test
    public void testContains() {
        _dbHashtable.remove(ELEMENT2);
        assertEquals("Check element 1", true, _dbHashtable.containsValue(ELEMENT1));
        assertEquals("Check element 2", true, _dbHashtable.containsValue(ELEMENT2));
        assertEquals("Check element 3", true, _dbHashtable.containsValue(ELEMENT3));
        assertEquals("Check no element", false, _dbHashtable.containsValue(KEY1));
        assertEquals("Check no element", false, _dbHashtable.containsValue(ELEMENT4));
    }

    /**
     * Test the addAll methods add the correct values.
     */
    @Test
    public void testPutAll() {
        HashMap<String, String> h= new HashMap<String, String>();
        h.put(KEY3, ELEMENT3);
        h.put(KEY4, ELEMENT4);
        h.put(KEY5, ELEMENT5);
        h.put(KEY6, ELEMENT6);
        
        _dbHashtable.putAll(h);
        assertTrue("Check the number of values", _dbHashtable.size()==6);
        assertEquals("Check element 1", ELEMENT1, _dbHashtable.get(KEY1));
        assertEquals("Check element 2", ELEMENT2, _dbHashtable.get(KEY2));
        assertEquals("Check element 3", ELEMENT3, _dbHashtable.get(KEY3));
        assertEquals("Check element 4", ELEMENT4, _dbHashtable.get(KEY4));
        assertEquals("Check element 5", ELEMENT5, _dbHashtable.get(KEY5));
        assertEquals("Check element 6", ELEMENT6, _dbHashtable.get(KEY6));
    }

    /**
     * Test the keySet method returns the correct set.
     */
    @Test
    public void testKeySet() {
        SortedSet<String> keys = new TreeSet<String>(_dbHashtable.keySet());
        assertTrue("Check the number of keys", keys.size()==3);
        String[] keyArray = keys.toArray(new String[0]);
        assertEquals("Check key 1", KEY1, keyArray[0]);
        assertEquals("Check key 2", KEY2, keyArray[1]);
        assertEquals("Check key 3", KEY3, keyArray[2]);
    }

    /**
     * Test the entrySet method returns the correct set.
     */
    @Test
    public void testEntrySet() {
        Set<Entry<String, String>> entries = _dbHashtable.entrySet();
        assertTrue("Check the number of entries", entries.size()==3);
        boolean b1 = false;
        boolean b2 = false;
        boolean b3 = false;
        for (Iterator<Entry<String, String>> i = 
            entries.iterator(); i.hasNext(); ) {
            Map.Entry<String, String> me = i.next();
            if (me.getKey() == KEY1) {
                assertEquals("Check entry 1", ELEMENT1, me.getValue());
                b1 = true;
            } else if (me.getKey() == KEY2) {
                assertEquals("Check entry 2", ELEMENT2, me.getValue());
                b2 = true;
            } else if (me.getKey() == KEY3) {
                assertEquals("Check entry 3", ELEMENT3, me.getValue());
                b3 = true;
            } else {
                fail ("Unexpected key found.");
            }
        }
        assertEquals("Check occurences of entries", b1 && b2 && b3, true);
    }

    /**
     * Test the values method returns the correct collection.
     */
    @Test
    public void testValues() {
        Collection<String> values = _dbHashtable.values();
        assertTrue("Check the number of values", values.size()==3);
    	HashSet<String> temp = new HashSet<String>();
    	temp.add(ELEMENT1);
    	temp.add(ELEMENT2);
    	temp.add(ELEMENT3);
        Iterator<String> i = values.iterator();
        while (i.hasNext()) {
        	assertTrue(temp.remove(i.next()));
        }
        assertTrue(temp.isEmpty());
    }
        
    /**
     * test batch loading.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchLoading() {
        System.out.println("Batch-test");
        PersistenceManager pm = null;
        Object oid = null;
        try {
    		pm = TestTools.openPM();
    		pm.currentTransaction().begin();
            DBHashtable<Object, Object> dbh = 
                new DBHashtable<Object, Object>();
            dbh.put("TestString", "TestString");
            for (int i = 0 ; i < 100; i++) {
                dbh.put("TS" + i, new PersistentDummyImpl());
            }
            dbh.put("TestString2", "TestString2");
            pm.makePersistent(dbh);
            oid = pm.getObjectId(dbh);
            pm.currentTransaction().commit(); 
            pm.close();
            pm = null;
        
    		pm = TestTools.openPM();
            dbh = (DBHashtable<Object, Object>) pm.getObjectById(oid);
            long t1 = System.currentTimeMillis();
            for (Object o: dbh.values()) {
                o.hashCode();
            }
            long t2 = System.currentTimeMillis();
            System.out.println("NORMAL: " + (t2 - t1));
            pm.close();
            pm = null;
        
    		pm = TestTools.openPM();
            dbh = (DBHashtable<Object, Object>) pm.getObjectById(oid);
            t1 = System.currentTimeMillis();
            dbh.setBatchSize(1000);
            for (Object o: dbh.values()) {
                o.hashCode();
            }
            t2 = System.currentTimeMillis();
            System.out.println("BATCHED: " + (t2 - t1));
            pm.close();
            pm = null;
        
            //Close the store and load the stuff
    		pm = TestTools.openPM();
            dbh = (DBHashtable<Object, Object>) pm.getObjectById(oid);
            t1 = System.currentTimeMillis();
            dbh.setBatchSize(1);
            for (Object o: dbh.values()) {
                o.hashCode();
            }
            t2 = System.currentTimeMillis();
            System.out.println("NORMAL: " + (t2 - t1));
            pm.close();
            pm = null;
        
            //Close the store and load the stuff
    		pm = TestTools.openPM();
            dbh = (DBHashtable<Object, Object>) pm.getObjectById(oid);
            t1 = System.currentTimeMillis();
            dbh.setBatchSize(0);
            for (Object o: dbh.values()) {
                o.hashCode();
            }
            t2 = System.currentTimeMillis();
            System.out.println("BATCHED: " + (t2 - t1));
            pm.close();
            pm = null;
            
            //Close the store, load the stuff and test with transient object
    		pm = TestTools.openPM();
            dbh = (DBHashtable<Object, Object>) pm.getObjectById(oid);
            PersistentDummyImpl dummyTrans = new PersistentDummyImpl();
            dbh.put("13", dummyTrans);
            t1 = System.currentTimeMillis();
            dbh.setBatchSize(0);
            for (Object o: dbh.values()) {
                o.hashCode();
            }
            t2 = System.currentTimeMillis();
            assertEquals(dummyTrans, dbh.get("13"));
            System.out.println("BATCHED: " + (t2 - t1));
            pm.close();
            pm = null;
            
            //Close the store, load the stuff and test with modified object
    		pm = TestTools.openPM();
            dbh = (DBHashtable<Object, Object>) pm.getObjectById(oid);
            ((PersistentDummyImpl)dbh.get("TS18")).setData(new byte[]{15});
            t1 = System.currentTimeMillis();
            dbh.setBatchSize(0);
            for (Object o: dbh.values()) {
                o.hashCode();
            }
            t2 = System.currentTimeMillis();
            assertEquals(15, ((PersistentDummyImpl)dbh.get("TS18")).getData()[0]);
            System.out.println("BATCHED but dirty: " + (t2 - t1));
        } finally {
            if (pm != null) {
                pm.close();
            }
        }
        //TODO use setBatch() also for all other tests to verify batch loading!
        //Or call these tests here again, outside the store.!
    }
}

package org.zoodb.test.api;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.zoodb.jdo.api.DBHashtable;

/**
 * Test harness for DBHashtable.
 *
 * @author  Tilmann Zaeschke
 */
public final class DBHashtableTest {

    private static final String DB_PROP = "zoodb.test.database";
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
     * The setUp method tests the put method of VHashtable.
     * @throws StoreException 
     */
    @Before
    protected void setUp() {
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
    protected void tearDown() {
        _dbHashtable.clear();
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
        Iterator<String> i = _dbHashtable.iterator();
        try {
            assertEquals("Check element 1", ELEMENT1, i.next());
            assertEquals("Check element 2", ELEMENT2, i.next());
            assertEquals("Check element 3", ELEMENT3, i.next());
        } catch (NoSuchElementException e) {
            fail("This shouldn't happen since _dbHastable should contain three elements");
        }
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
        String[] valueArray = values.toArray(new String[0]);
        assertEquals("Check value 1", ELEMENT1, valueArray[0]);
        assertEquals("Check value 2", ELEMENT2, valueArray[1]);
        assertEquals("Check value 3", ELEMENT3, valueArray[2]);
    }
        
    /**
     * test batch loading.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchLoading() {
        System.out.println("Batch-test");
        ObjectStore os = null;
        Object oid = null;
        try {
            os = StoreFactory.create().createStore(DB_PROP);
            DBHashtable<Object, Object> dbh = 
                new DBHashtable<Object, Object>();
            dbh.put("TestString", "TestString");
            for (int i = 0 ; i < 100; i++) {
                dbh.put("TS" + i, new PersistentDummyImpl());
            }
            dbh.put("TestString2", "TestString2");
            os.makePersistent(dbh);
            oid = os.getObjectId(dbh);
            os.commit(); 
            os.exit();
            os = null;
        
            os = StoreFactory.create().createStore(DB_PROP);
            dbh = (DBHashtable<Object, Object>) os.getObjectById(oid);
            long t1 = System.currentTimeMillis();
            for (Object o: dbh.values()) {
                o.hashCode();
            }
            long t2 = System.currentTimeMillis();
            System.out.println("NORMAL: " + (t2 - t1));
            os.exit();
            os = null;
        
            os = StoreFactory.create().createStore(DB_PROP);
            dbh = (DBHashtable<Object, Object>) os.getObjectById(oid);
            t1 = System.currentTimeMillis();
            dbh.setBatchSize(1000);
            for (Object o: dbh.values()) {
                o.hashCode();
            }
            t2 = System.currentTimeMillis();
            System.out.println("BATCHED: " + (t2 - t1));
            os.exit();
            os = null;
        
            //Close the store and load the stuff
            os = StoreFactory.create().createStore(DB_PROP);
            dbh = (DBHashtable<Object, Object>) os.getObjectById(oid);
            t1 = System.currentTimeMillis();
            dbh.setBatchSize(1);
            for (Object o: dbh.values()) {
                o.hashCode();
            }
            t2 = System.currentTimeMillis();
            System.out.println("NORMAL: " + (t2 - t1));
            os.exit();
            os = null;
        
            //Close the store and load the stuff
            os = StoreFactory.create().createStore(DB_PROP);
            dbh = (DBHashtable<Object, Object>) os.getObjectById(oid);
            t1 = System.currentTimeMillis();
            dbh.setBatchSize(0);
            for (Object o: dbh.values()) {
                o.hashCode();
            }
            t2 = System.currentTimeMillis();
            System.out.println("BATCHED: " + (t2 - t1));
            os.exit();
            os = null;
            
            //Close the store, load the stuff and test with transient object
            os = StoreFactory.create().createStore(DB_PROP);
            dbh = (DBHashtable<Object, Object>) os.getObjectById(oid);
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
            os.exit();
            os = null;
            
            //Close the store, load the stuff and test with modified object
            os = StoreFactory.create().createStore(DB_PROP);
            dbh = (DBHashtable<Object, Object>) os.getObjectById(oid);
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
            if (os != null) {
                os.exit();
            }
        }
        //TODO use setBatch() also for all other tests to verify batch loading!
        //Or call these tests here again, outside the store.!
    }
}

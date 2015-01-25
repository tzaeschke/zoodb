/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.test.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
import org.zoodb.api.DBArrayList;
import org.zoodb.api.DBHashMap;
import org.zoodb.test.testutil.TestTools;

/**
 * Test harness for DBHashtable.
 *
 * @author  Tilmann Zaeschke
 */
public final class DBHashMapTest {

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
    
    private DBHashMap<String, String> dbHashtable;
    
    /**
     * Run before each test.
     * The setUp method tests the put method of DBHashtable.
     */
    @Before
    public void before() {
        //create a DBHashtable
        dbHashtable = new DBHashMap<String, String>();
        dbHashtable.put(KEY1, ELEMENT1);
        dbHashtable.put(KEY2, ELEMENT2);
        dbHashtable.put(KEY3, ELEMENT3);
    }

    /**
     * Run after each test.
     */
    @After
    public void after() {
        dbHashtable.clear();
        TestTools.closePM();
    }

    @BeforeClass
    public static void beforeClass() {
    	TestTools.createDb();
    	TestTools.defineSchema(PersistentDummyImpl.class);
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
        assertEquals("Check size 1", 3, dbHashtable.size());
        dbHashtable.clear();
        assertEquals("Check size 2", 0, dbHashtable.size());
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
        Iterator<String> i = dbHashtable.values().iterator();
        while (i.hasNext()) {
        	assertTrue(temp.remove(i.next()));
        }
        assertTrue(temp.isEmpty());
        assertFalse("Check the number of remaining elements", i.hasNext());
    }

    /**
     * Test the remove method removes the correct values.
     */
    @Test
    public void testRemove() {
        dbHashtable.remove(KEY2);
        assertEquals("Check element 1", ELEMENT1, dbHashtable.get(KEY1));
        assertEquals("Check element 2", null, dbHashtable.get(KEY2));
        assertEquals("Check element 3", ELEMENT3, dbHashtable.get(KEY3));
        assertTrue("Check the number of values", dbHashtable.size()==2);
    }

    /**
     * Test the contains method returns the correct results.
     */
    @Test
    public void testContains() {
        dbHashtable.remove(ELEMENT2);
        assertEquals("Check element 1", true, dbHashtable.containsValue(ELEMENT1));
        assertEquals("Check element 2", true, dbHashtable.containsValue(ELEMENT2));
        assertEquals("Check element 3", true, dbHashtable.containsValue(ELEMENT3));
        assertEquals("Check no element", false, dbHashtable.containsValue(KEY1));
        assertEquals("Check no element", false, dbHashtable.containsValue(ELEMENT4));
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
        
        dbHashtable.putAll(h);
        assertTrue("Check the number of values", dbHashtable.size()==6);
        assertEquals("Check element 1", ELEMENT1, dbHashtable.get(KEY1));
        assertEquals("Check element 2", ELEMENT2, dbHashtable.get(KEY2));
        assertEquals("Check element 3", ELEMENT3, dbHashtable.get(KEY3));
        assertEquals("Check element 4", ELEMENT4, dbHashtable.get(KEY4));
        assertEquals("Check element 5", ELEMENT5, dbHashtable.get(KEY5));
        assertEquals("Check element 6", ELEMENT6, dbHashtable.get(KEY6));
    }

    /**
     * Test the keySet method returns the correct set.
     */
    @Test
    public void testKeySet() {
        SortedSet<String> keys = new TreeSet<String>(dbHashtable.keySet());
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
        Set<Entry<String, String>> entries = dbHashtable.entrySet();
        assertTrue("Check the number of entries", entries.size()==3);
        boolean b1 = false;
        boolean b2 = false;
        boolean b3 = false;
        for (Iterator<Entry<String, String>> i = 
            entries.iterator(); i.hasNext(); ) {
            Map.Entry<String, String> me = i.next();
            if (me.getKey().equals(KEY1)) {
                assertEquals("Check entry 1", ELEMENT1, me.getValue());
                b1 = true;
            } else if (me.getKey().equals(KEY2)) {
                assertEquals("Check entry 2", ELEMENT2, me.getValue());
                b2 = true;
            } else if (me.getKey().equals(KEY3)) {
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
        Collection<String> values = dbHashtable.values();
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

        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        DBHashMap<Object, Object> dbh = 
            new DBHashMap<Object, Object>();
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
        pm.currentTransaction().begin();
        dbh = (DBHashMap<Object, Object>) pm.getObjectById(oid);
        long t1 = System.currentTimeMillis();
        for (Object o: dbh.values()) {
            o.hashCode();
        }
        long t2 = System.currentTimeMillis();
        System.out.println("NORMAL: " + (t2 - t1));
        pm.currentTransaction().commit(); 
        pm.close();
        pm = null;

        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        dbh = (DBHashMap<Object, Object>) pm.getObjectById(oid);
        t1 = System.currentTimeMillis();
        dbh.setBatchSize(1000);
        for (Object o: dbh.values()) {
            o.hashCode();
        }
        t2 = System.currentTimeMillis();
        System.out.println("BATCHED: " + (t2 - t1));
        pm.currentTransaction().commit(); 
        pm.close();
        pm = null;

        //Close the store and load the stuff
        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        dbh = (DBHashMap<Object, Object>) pm.getObjectById(oid);
        t1 = System.currentTimeMillis();
        dbh.setBatchSize(1);
        for (Object o: dbh.values()) {
            o.hashCode();
        }
        t2 = System.currentTimeMillis();
        System.out.println("NORMAL: " + (t2 - t1));
        pm.currentTransaction().commit(); 
        pm.close();
        pm = null;

        //Close the store and load the stuff
        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        dbh = (DBHashMap<Object, Object>) pm.getObjectById(oid);
        t1 = System.currentTimeMillis();
        dbh.setBatchSize(0);
        for (Object o: dbh.values()) {
            o.hashCode();
        }
        t2 = System.currentTimeMillis();
        System.out.println("BATCHED: " + (t2 - t1));
        pm.currentTransaction().commit(); 
        pm.close();
        pm = null;

        //Close the store, load the stuff and test with transient object
        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        dbh = (DBHashMap<Object, Object>) pm.getObjectById(oid);
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
        pm.currentTransaction().commit(); 
        pm.close();
        pm = null;

        //Close the store, load the stuff and test with modified object
        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        dbh = (DBHashMap<Object, Object>) pm.getObjectById(oid);
        ((PersistentDummyImpl)dbh.get("TS18")).setData(new byte[]{15});
        t1 = System.currentTimeMillis();
        dbh.setBatchSize(0);
        for (Object o: dbh.values()) {
            o.hashCode();
        }
        t2 = System.currentTimeMillis();
        assertEquals(15, ((PersistentDummyImpl)dbh.get("TS18")).getData()[0]);
        System.out.println("BATCHED but dirty: " + (t2 - t1));
        pm.currentTransaction().rollback();
        TestTools.closePM();

        //TODO use setBatch() also for all other tests to verify batch loading!
        //Or call these tests here again, outside the store.!
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testPersistent() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        
        DBHashMap<DBArrayList<String>, DBArrayList<String>> map = 
            new DBHashMap<DBArrayList<String>, DBArrayList<String>>();
        DBArrayList<String> a1 = new DBArrayList<String>();
        a1.add("a1");
        DBArrayList<String> a2 = new DBArrayList<String>();
        a2.add("a2");
        map.put(a1, a2);
        
        pm.makePersistent(map);
        Object oid = pm.getObjectId(map);
        
        pm.currentTransaction().commit();
        TestTools.closePM();
        
        
        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        
        map = (DBHashMap<DBArrayList<String>, DBArrayList<String>>) pm.getObjectById(oid);
        Set<DBArrayList<String>> ks = map.keySet();
        Iterator<DBArrayList<String>> iter = ks.iterator(); 
        assertTrue(iter.hasNext());
        a1 = iter.next();
        assertEquals(a1.get(0), "a1");
        assertFalse(iter.hasNext());
        
        
        a2 = map.get(a1);
        assertEquals(a2.get(0), "a2");

        pm.currentTransaction().commit();
        TestTools.closePM();
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testPersistentNull() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        
        DBHashMap<DBArrayList<String>, DBArrayList<String>> map = 
            new DBHashMap<DBArrayList<String>, DBArrayList<String>>();
        DBArrayList<String> a1 = new DBArrayList<String>();
        a1.add("a1");
        DBArrayList<String> a2 = new DBArrayList<String>();
        a2.add("a2");
        map.put(a1, null);
        map.put(null, a2);
        
        pm.makePersistent(map);
        Object oid = pm.getObjectId(map);
        
        pm.currentTransaction().commit();
        TestTools.closePM();
        
        
        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        
        map = (DBHashMap<DBArrayList<String>, DBArrayList<String>>) pm.getObjectById(oid);
        Set<DBArrayList<String>> ks = map.keySet();
        Iterator<DBArrayList<String>> iter = ks.iterator(); 
        assertTrue(iter.hasNext());
        a1 = iter.next();
        if (a1 != null) {
            assertEquals(a1.get(0), "a1");
            assertNull(iter.next());
        } else {
            a1 = iter.next();
            assertEquals(a1.get(0), "a1");
        }
        assertFalse(iter.hasNext());
        
        
        assertNull(map.get(a1));
        a2 = map.get(null);
        assertEquals(a2.get(0), "a2");

        pm.currentTransaction().commit();
        TestTools.closePM();
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testPersistentSelf() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        
        DBHashMap<DBHashMap<?,?>, DBHashMap<?,?>> map0 = 
            new DBHashMap<DBHashMap<?,?>, DBHashMap<?,?>>();
        DBHashMap<DBHashMap<?,?>, DBHashMap<?,?>> map1 = 
            new DBHashMap<DBHashMap<?,?>, DBHashMap<?,?>>();
        DBHashMap<DBHashMap<?,?>, DBHashMap<?,?>> map2 = 
            new DBHashMap<DBHashMap<?,?>, DBHashMap<?,?>>();
        map0.put(map0, map1);
        map0.put(map2, map0);
        
        pm.makePersistent(map0);
        Object oid0 = pm.getObjectId(map0);
        pm.makePersistent(map1);
        Object oid1 = pm.getObjectId(map1);
        pm.makePersistent(map2);
        Object oid2 = pm.getObjectId(map2);
        
        pm.currentTransaction().commit();
        TestTools.closePM();
        
        
        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        
        map0 = (DBHashMap<DBHashMap<?,?>, DBHashMap<?,?>>) pm.getObjectById(oid0);
        map2 = (DBHashMap<DBHashMap<?,?>, DBHashMap<?,?>>) pm.getObjectById(oid2);
        assertEquals(2, map0.size());
        map1 = (DBHashMap<DBHashMap<?, ?>, DBHashMap<?, ?>>) map0.get(map0);
        assertNotNull(map1);
        assertEquals(oid1, pm.getObjectId(map1));
        assertEquals(map0, map0.get(map2));

        pm.currentTransaction().commit();
        TestTools.closePM();
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testPersistentSelfWithLoop() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        
        DBHashMap<DBHashMap<?,?>, DBHashMap<?,?>> map0 = 
            new DBHashMap<DBHashMap<?,?>, DBHashMap<?,?>>();
        DBHashMap<DBHashMap<?,?>, DBHashMap<?,?>> map1 = 
            new DBHashMap<DBHashMap<?,?>, DBHashMap<?,?>>();
        DBHashMap<DBHashMap<?,?>, DBHashMap<?,?>> map2 = 
            new DBHashMap<DBHashMap<?,?>, DBHashMap<?,?>>();
        map0.put(map0, null);
        map0.put(map1, null);
        map1.put(map0, map2);
        map1.put(map1, map2);
        map2.put(map2, map2);
        
        pm.makePersistent(map0);
        pm.makePersistent(map1);
        pm.makePersistent(map2);
        Object oid0 = pm.getObjectId(map0);
        Object oid1 = pm.getObjectId(map1);
        Object oid2 = pm.getObjectId(map2);
        
        pm.currentTransaction().commit();
        TestTools.closePM();
        
        
        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        
        map0 = (DBHashMap<DBHashMap<?,?>, DBHashMap<?,?>>) pm.getObjectById(oid0);
        map1 = (DBHashMap<DBHashMap<?,?>, DBHashMap<?,?>>) pm.getObjectById(oid1);
        map2 = (DBHashMap<DBHashMap<?,?>, DBHashMap<?,?>>) pm.getObjectById(oid2);
        assertEquals(2, map0.size());
        assertEquals(2, map1.size());
        assertEquals(null, map0.get(map0));
        assertEquals(null, map0.get(map1));
        assertEquals(map2, map1.get(map0));
        assertEquals(map2, map1.get(map1));
        assertEquals(map2, map2.get(map2));

        pm.currentTransaction().commit();
        TestTools.closePM();
    }
    
}

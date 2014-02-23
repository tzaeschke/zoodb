/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;

import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.zoodb.jdo.api.DBArrayList;
import org.zoodb.test.testutil.TestTools;

/**
 * Test harness for DBVector.
 *
 * @author  Tilmann Zaeschke
 */
public final class DBArrayListTest {

    private final static String DB_NAME = "TestDb";
    private static final String ELEMENT1 = "element one";
    private static final String ELEMENT2 = "second element";
    private static final String ELEMENT3 = "another element";
    
    private DBArrayList<String> _dbVector;
    
    @BeforeClass
    public static void setUpClass() {
        TestTools.createDb(DB_NAME);
        TestTools.defineSchema(DB_NAME, PersistentDummyImpl.class);
    }
    
    @AfterClass
    public static void tearDownClass() {
        TestTools.removeDb(DB_NAME);
    }
   
    /**
     * Run before each test.
     * The setUp method tests the put method.
     */
    @Before
    public void before() {
        //create a DBHashtable
        _dbVector = new DBArrayList<String>();
        _dbVector.add(ELEMENT1);
        _dbVector.add(ELEMENT2);
        _dbVector.add(ELEMENT3);
    }

    /**
     * Run after each test.
     */
    @After
    public void after() {
        _dbVector.clear();
    }

    /**
     * Test the clear() and size() methods.
     */
    @Test
    public void testClearAndSize() {
        assertEquals("Check size 1", 3, _dbVector.size());
        _dbVector.clear();
        assertEquals("Check size 2", 0, _dbVector.size());
    }
    
    /**
     * Test the iterator method iterates over the correct collection.
     */
    @Test
    public void testIterator() {
        Iterator<String> i = _dbVector.iterator();
        try {
            assertEquals("Check element 1", ELEMENT1, i.next());
            assertEquals("Check element 2", ELEMENT2, i.next());
            assertEquals("Check element 3", ELEMENT3, i.next());
        } catch (NoSuchElementException e) {
            fail("This shouldn't happen since _dbHastable should contain " +
                    "three elements");
        }
        assertFalse("Check the number of remaining elements", i.hasNext());
    }

    /**
     * Test the get method returns the correct values.
     */
    @Test
    public void testGet() {
        assertEquals("Check element 1", ELEMENT1, _dbVector.get(0));
        assertEquals("Check element 2", ELEMENT2, _dbVector.get(1));
        assertEquals("Check element 3", ELEMENT3, _dbVector.get(2));
    }

    /**
     * Test the set method sets the correct values.
     */
    @Test
    public void testSet() {
        _dbVector.set(0, ELEMENT2);
        _dbVector.set(1, ELEMENT1);
        assertTrue("Check the number of values", _dbVector.size()==3);
        assertEquals("Check element 1", ELEMENT1, _dbVector.get(1));
        assertEquals("Check element 2", ELEMENT2, _dbVector.get(0));
        assertEquals("Check element 3", ELEMENT3, _dbVector.get(2));
    }

    /**
     * Test the remove method removes the correct values.
     */
    @Test
    public void testRemove() {
        _dbVector.remove(1);
        assertEquals("Check element 1", ELEMENT1, _dbVector.get(0));
        assertEquals("Check element 3", ELEMENT3, _dbVector.get(1));
        assertTrue("Check the number of values", _dbVector.size()==2);
    }

    /**
     * Test the remove method removes the correct values.
     */
    @Test
    public void testRemove2() {
        _dbVector.remove(ELEMENT2);
        assertEquals("Check element 1", ELEMENT1, _dbVector.get(0));
        assertEquals("Check element 3", ELEMENT3, _dbVector.get(1));
        assertTrue("Check the number of values", _dbVector.size()==2);
    }

    /**
     * Test the addAll methods add the correct values.
     */

    @Test
    public void testAddAll() {
        LinkedList<String> l = new LinkedList<String>();
        l.add(ELEMENT3);
        l.add(ELEMENT2);
        l.add(ELEMENT1);
        
        _dbVector.addAll(l);
        assertTrue("Check the number of values", _dbVector.size()==6);
        assertEquals("Check element 1", ELEMENT1, _dbVector.get(0));
        assertEquals("Check element 2", ELEMENT2, _dbVector.get(1));
        assertEquals("Check element 3", ELEMENT3, _dbVector.get(2));
        assertEquals("Check element 3", ELEMENT3, _dbVector.get(3));
        assertEquals("Check element 2", ELEMENT2, _dbVector.get(4));
        assertEquals("Check element 1", ELEMENT1, _dbVector.get(5));

        _dbVector.addAll(1, l);
        assertTrue("Check the number of values", _dbVector.size()==9);
        assertEquals("Check element 1", ELEMENT1, _dbVector.get(0));
        assertEquals("Check element 3", ELEMENT3, _dbVector.get(1));
        assertEquals("Check element 2", ELEMENT2, _dbVector.get(2));
        assertEquals("Check element 1", ELEMENT1, _dbVector.get(3));
        assertEquals("Check element 2", ELEMENT2, _dbVector.get(4));
        assertEquals("Check element 3", ELEMENT3, _dbVector.get(5));
        assertEquals("Check element 3", ELEMENT3, _dbVector.get(6));
        assertEquals("Check element 2", ELEMENT2, _dbVector.get(7));
        assertEquals("Check element 1", ELEMENT1, _dbVector.get(8));
    }

    /**
     * Test the values method returns the correct collection.
     */
    @Test
    public void testToArray() {
        Object[] arrayO =  _dbVector.toArray();
        assertTrue("Check the number of values", arrayO.length==3);
        assertEquals("Check value 1", ELEMENT1, arrayO[0]);
        assertEquals("Check value 2", ELEMENT2, arrayO[1]);
        assertEquals("Check value 3", ELEMENT3, arrayO[2]);

        String[] array = _dbVector.toArray(new String[0]);
        assertTrue("Check the number of values", array.length==3);
        assertEquals("Check value 1", ELEMENT1, array[0]);
        assertEquals("Check value 2", ELEMENT2, array[1]);
        assertEquals("Check value 3", ELEMENT3, array[2]);

        array = _dbVector.toArray(new String[10]);
        assertTrue("Check the number of values", array.length==10);
        assertEquals("Check value 1", ELEMENT1, array[0]);
        assertEquals("Check value 2", ELEMENT2, array[1]);
        assertEquals("Check value 3", ELEMENT3, array[2]);
        assertEquals("Check value 4", null, array[3]);
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
        DBArrayList<Object> dbv = new DBArrayList<Object>();
        dbv.add("TestString");
        for (int i = 0 ; i < 100; i++) {
            dbv.add(new PersistentDummyImpl());
        }
        dbv.add("TestString2");
        pm.makePersistent(dbv);
        oid = pm.getObjectId(dbv);
        pm.currentTransaction().commit(); 
        pm.close();
        pm = null;

        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        dbv = (DBArrayList<Object>) pm.getObjectById(oid);
        long t1 = System.currentTimeMillis();
        for (Object o: dbv) {
            o.hashCode();
        }
        long t2 = System.currentTimeMillis();
        System.out.println("NORMAL: " + (t2 - t1));
        pm.currentTransaction().commit(); 
        pm.close();
        pm = null;

        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        dbv = (DBArrayList<Object>) pm.getObjectById(oid);
        t1 = System.currentTimeMillis();
        dbv.setBatchSize(1000);
        for (Object o: dbv) {
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
        dbv = (DBArrayList<Object>) pm.getObjectById(oid);
        t1 = System.currentTimeMillis();
        dbv.setBatchSize(1);
        for (Object o: dbv) {
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
        dbv = (DBArrayList<Object>) pm.getObjectById(oid);
        t1 = System.currentTimeMillis();
        dbv.setBatchSize(0);
        for (Object o: dbv) {
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
        dbv = (DBArrayList<Object>) pm.getObjectById(oid);
        PersistentDummyImpl dummyTrans = new PersistentDummyImpl();
        dbv.add(13, dummyTrans);
        t1 = System.currentTimeMillis();
        dbv.setBatchSize(0);
        for (Object o: dbv) {
            o.hashCode();
        }
        t2 = System.currentTimeMillis();
        assertEquals(dummyTrans, dbv.get(13));
        System.out.println("BATCHED: " + (t2 - t1));
        pm.currentTransaction().commit(); 
        pm.close();
        pm = null;

        //Close the store, load the stuff and test with modified object
        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        dbv = (DBArrayList<Object>) pm.getObjectById(oid);
        ((PersistentDummyImpl)dbv.get(18)).setData(new byte[]{15});
        t1 = System.currentTimeMillis();
        dbv.setBatchSize(0);
        for (Object o: dbv) {
            o.hashCode();
        }
        t2 = System.currentTimeMillis();
        assertEquals(15, ((PersistentDummyImpl)dbv.get(18)).getData()[0]);
        System.out.println("BATCHED but dirty: " + (t2 - t1));
        pm.currentTransaction().rollback();
        TestTools.closePM();

        //TODO use setBatch() also for all other tests to verify batch loading!
        //Or call these tests here again, outside the store.!
    }
    
    /**
     * test batch loading.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchLoading2() {
        System.out.println("Batch-test 2");
        PersistenceManager pm = null;
        Object oid = null;
        try {
    		pm = TestTools.openPM();
    		pm.currentTransaction().begin();
            DBArrayList<Object> dbv = new DBArrayList<Object>();
            for (int i = 0 ; i < 120; i++) {
                dbv.add(new PersistentDummyImpl());
            }
            pm.makePersistent(dbv);
            oid = pm.getObjectId(dbv);
            pm.currentTransaction().commit(); 
            pm.close();
            pm = null;
        
    		pm = TestTools.openPM();
    		pm.currentTransaction().begin();
            dbv = (DBArrayList<Object>) pm.getObjectById(oid);
            dbv.setBatchSize(110);
            for (Object o: dbv) {
                o.getClass();
            }
            pm.currentTransaction().commit(); 
        } finally {
            if (pm != null) {
                pm.close();
            }
        }
    }
}

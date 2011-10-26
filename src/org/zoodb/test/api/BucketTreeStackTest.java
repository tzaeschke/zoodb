package org.zoodb.test.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.internal.util.BucketTreeStack;

/**
 * Test harness for BucketArrayList.
 *
 * @author  Tilmann Zaeschke
 */
public final class BucketTreeStackTest {

    private static final String ELEMENT1 = "element one";
    private static final String ELEMENT2 = "second element";
    private static final String ELEMENT3 = "another element";
    
    private BucketTreeStack<String> _bucket;
    
    /**
     * Run before each test.
     * The setUp method tests the put method.
     * @throws StoreException
     */
    @Before
    public void before() {
        //create a DBHashtable
        _bucket = new BucketTreeStack<String>();
        _bucket.add(ELEMENT1);
        _bucket.add(ELEMENT2);
        _bucket.add(ELEMENT3);
    }

    /**
     * Run after each test.
     */
    @After
    public void after() {
        _bucket.clear();
    }

    /**
     * Test the clear() and size() methods.
     */
    @Test
    public void testClearAndSize() {
        assertEquals("Check size 1", 3, _bucket.size());
        _bucket.clear();
        assertEquals("Check size 2", 0, _bucket.size());
    }
    
    /**
     * Test the iterator method iterates over the correct collection.
     */
    @Test
    public void testIterator() {
        Iterator<String> i = _bucket.iterator();
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
        assertEquals("Check element 1", ELEMENT1, _bucket.get(0));
        assertEquals("Check element 2", ELEMENT2, _bucket.get(1));
        assertEquals("Check element 3", ELEMENT3, _bucket.get(2));
    }

    /**
     * Test the set method sets the correct values.
     */
    @Test
    public void testSet() {
        _bucket.set(0, ELEMENT2);
        _bucket.set(1, ELEMENT1);
        assertEquals("Check the number of values", 3, _bucket.size());
        assertEquals("Check element 1", ELEMENT1, _bucket.get(1));
        assertEquals("Check element 2", ELEMENT2, _bucket.get(0));
        assertEquals("Check element 3", ELEMENT3, _bucket.get(2));
    }

    /**
     * Test the set method sets the correct values.
     */
    @Test
    public void testSetMany() {
    	final int MAX = 2000000;
    	BucketTreeStack<Integer> bucket = new BucketTreeStack<Integer>();
    	for (int i = 0; i < MAX; i++) {
    		bucket.add(i);
    		assertEquals(i+1, bucket.size());
    		assertEquals(i, (int)bucket.get(i));
    	}
    	for (int i = 0; i < MAX; i++) {
    		assertEquals(i, (int)bucket.get(i));
    	}
    }

    /**
     * Test the remove method removes the correct values.
     */
    @Test
    public void testRemove() {
        _bucket.removeLast();
        assertEquals("Check element 1", ELEMENT1, _bucket.get(0));
        assertEquals("Check element 2", ELEMENT2, _bucket.get(1));
        assertTrue("Check the number of values", _bucket.size()==2);
    }

    @Test
    public void testIsEmpty() {
    	_bucket.clear();
    	assertTrue(_bucket.isEmpty());
    	assertEquals(0, _bucket.size());
    	
    	_bucket.add("X");
    	assertFalse(_bucket.isEmpty());
    	assertEquals(1, _bucket.size());
    	
    	_bucket.clear();
    	assertTrue(_bucket.isEmpty());
    	assertEquals(0, _bucket.size());
    }

    //    /**
//     * Test the remove method removes the correct values.
//     */
//    @Test
//    public void testRemove2() {
//        _bucket.remove(ELEMENT2);
//        assertEquals("Check element 1", ELEMENT1, _bucket.get(0));
//        assertEquals("Check element 3", ELEMENT3, _bucket.get(1));
//        assertTrue("Check the number of values", _bucket.size()==2);
//    }
//
//    /**
//     * Test the addAll methods add the correct values.
//     */
//
//    @Test
//    public void testAddAll() {
//        LinkedList<String> l = new LinkedList<String>();
//        l.add(ELEMENT3);
//        l.add(ELEMENT2);
//        l.add(ELEMENT1);
//        
//        _bucket.addAll(l);
//        assertTrue("Check the number of values", _bucket.size()==6);
//        assertEquals("Check element 1", ELEMENT1, _bucket.get(0));
//        assertEquals("Check element 2", ELEMENT2, _bucket.get(1));
//        assertEquals("Check element 3", ELEMENT3, _bucket.get(2));
//        assertEquals("Check element 3", ELEMENT3, _bucket.get(3));
//        assertEquals("Check element 2", ELEMENT2, _bucket.get(4));
//        assertEquals("Check element 1", ELEMENT1, _bucket.get(5));
//
//        _bucket.addAll(1, l);
//        assertTrue("Check the number of values", _bucket.size()==9);
//        assertEquals("Check element 1", ELEMENT1, _bucket.get(0));
//        assertEquals("Check element 3", ELEMENT3, _bucket.get(1));
//        assertEquals("Check element 2", ELEMENT2, _bucket.get(2));
//        assertEquals("Check element 1", ELEMENT1, _bucket.get(3));
//        assertEquals("Check element 2", ELEMENT2, _bucket.get(4));
//        assertEquals("Check element 3", ELEMENT3, _bucket.get(5));
//        assertEquals("Check element 3", ELEMENT3, _bucket.get(6));
//        assertEquals("Check element 2", ELEMENT2, _bucket.get(7));
//        assertEquals("Check element 1", ELEMENT1, _bucket.get(8));
//    }
//
//    /**
//     * Test the values method returns the correct collection.
//     */
//    @Test
//    public void testToArray() {
//        Object[] arrayO =  _bucket.toArray();
//        assertEquals("Check the number of values", 3, arrayO.length);
//        assertEquals("Check value 1", ELEMENT1, arrayO[0]);
//        assertEquals("Check value 2", ELEMENT2, arrayO[1]);
//        assertEquals("Check value 3", ELEMENT3, arrayO[2]);
//
//        String[] array = _bucket.toArray(new String[0]);
//        assertEquals("Check the number of values", 3, array.length);
//        assertEquals("Check value 1", ELEMENT1, array[0]);
//        assertEquals("Check value 2", ELEMENT2, array[1]);
//        assertEquals("Check value 3", ELEMENT3, array[2]);
//
//        array = _bucket.toArray(new String[10]);
//        assertEquals("Check the number of values", 10, array.length);
//        assertEquals("Check value 1", ELEMENT1, array[0]);
//        assertEquals("Check value 2", ELEMENT2, array[1]);
//        assertEquals("Check value 3", ELEMENT3, array[2]);
//        assertEquals("Check value 4", null, array[3]);
//    }
       
}

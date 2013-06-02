/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.test.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.internal.util.WeakIdentityHashMap;

/**
 * Test harness for PrimLongTreeMap.
 *
 * @author  Tilmann Zaeschke
 */
public final class WeakIdentityHashMapTest {

    private static final Long KEY1 = Long.valueOf(1);
    private static final Long KEY2 = Long.valueOf(2);
    private static final Long KEY3 = Long.valueOf(3);
    private static final Long KEY4 = Long.valueOf(4);
    private static final Long KEY5 = Long.valueOf(5);
    private static final Long KEY6 = Long.valueOf(6);
    private static final String ELEMENT1 = "first one";
    private static final String ELEMENT2 = "second element";
    private static final String ELEMENT3 = "third element";
    private static final String ELEMENT4 = "fourth element";
    private static final String ELEMENT5 = "fifth element";
    private static final String ELEMENT6 = "sixth element";
    
    private WeakIdentityHashMap<Long, String> map;
    
    /**
     * Run before each test.
     * The setUp method tests the put method of PrimLongTreeMap.
     * @throws StoreException 
     */
    @Before
    public void before() {
        //create a PrimLongTreeMap
        map = new WeakIdentityHashMap<Long, String>();
        map.put(KEY1, ELEMENT1);
        map.put(KEY2, ELEMENT2);
        map.put(KEY3, ELEMENT3);
    }

    /**
     * Run after each test.
     */
    @After
    public void after() {
        map.clear();
    }

    /**
     * Test the clear() and size() methods.
     */
    @Test
    public void testClearAndSize() {
        assertEquals("Check size 1", 3, map.size());
        map.clear();
        assertEquals("Check size 2", 0, map.size());
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
        Iterator<String> i = map.values().iterator();
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
        map.remove(KEY2);
        assertEquals("Check element 1", ELEMENT1, map.get(KEY1));
        assertEquals("Check element 2", null, map.get(KEY2));
        assertEquals("Check element 3", ELEMENT3, map.get(KEY3));
        assertTrue("Check the number of values", map.size()==2);
    }

    /**
     * Test the contains method returns the correct results.
     */
    @Test
    public void testContains() {
        map.remove(KEY2);
        assertEquals("Check no element", true, map.containsKey(KEY1));
        assertEquals("Check no element", false, map.containsKey(KEY2));
        assertEquals("Check element 1", true, map.containsValue(ELEMENT1));
        assertEquals("Check element 2", false, map.containsValue(ELEMENT2));
        assertEquals("Check element 3", true, map.containsValue(ELEMENT3));
        //assertEquals("Check no element", false, map.containsValue(KEY1));
        assertEquals("Check no element", false, map.containsValue(ELEMENT4));
    }

    /**
     * Test the addAll methods add the correct values.
     */
    @Test
    public void testPutAll() {
        HashMap<Long, String> h= new HashMap<Long, String>();
        h.put(KEY3, ELEMENT3);
        h.put(KEY4, ELEMENT4);
        h.put(KEY5, ELEMENT5);
        h.put(KEY6, ELEMENT6);
        
        map.putAll(h);
        assertTrue("Check the number of values", map.size()==6);
        assertEquals("Check element 1", ELEMENT1, map.get(KEY1));
        assertEquals("Check element 2", ELEMENT2, map.get(KEY2));
        assertEquals("Check element 3", ELEMENT3, map.get(KEY3));
        assertEquals("Check element 4", ELEMENT4, map.get(KEY4));
        assertEquals("Check element 5", ELEMENT5, map.get(KEY5));
        assertEquals("Check element 6", ELEMENT6, map.get(KEY6));
    }

    /**
     * Test the keySet method returns the correct set.
     */
    @Test
    public void testKeySet() {
        SortedSet<Long> keys = new TreeSet<Long>(map.keySet());
        assertTrue("Check the number of keys", keys.size()==3);
        Long[] keyArray = keys.toArray(new Long[0]);
        assertEquals("Check key 1", KEY1, keyArray[0]);
        assertEquals("Check key 2", KEY2, keyArray[1]);
        assertEquals("Check key 3", KEY3, keyArray[2]);
    }

    /**
     * Test the entrySet method returns the correct set.
     */
    @Test
    public void testEntrySet() {
        Set<Map.Entry<Long, String>> entries = map.entrySet();
        assertTrue("Check the number of entries", entries.size()==3);
        boolean b1 = false;
        boolean b2 = false;
        boolean b3 = false;
        for (Iterator<Map.Entry<Long, String>> i = 
            entries.iterator(); i.hasNext(); ) {
        	Map.Entry<Long, String> me = i.next();
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
        Collection<String> values = map.values();
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
    
}

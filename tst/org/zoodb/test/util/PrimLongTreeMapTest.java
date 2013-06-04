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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.internal.util.PrimLongTreeMap;

/**
 * Test harness for PrimLongTreeMap.
 *
 * @author  Tilmann Zaeschke
 */
public final class PrimLongTreeMapTest {

    private static final long KEY0 = 0;
    private static final long KEY1 = 1;
    private static final long KEY2 = 2;
    private static final long KEY3 = 3;
    private static final long KEY4 = 4;
//    private static final long KEY5 = 5;
//    private static final long KEY6 = 6;
    private static final String ELEMENT1 = "first one";
    private static final String ELEMENT2 = "second element";
    private static final String ELEMENT3 = "third element";
    private static final String ELEMENT4 = "fourth element";
//    private static final String ELEMENT5 = "fifth element";
//    private static final String ELEMENT6 = "sixth element";
    
    private PrimLongTreeMap<String> map;
    
    /**
     * Run before each test.
     * The setUp method tests the put method of PrimLongTreeMap.
     * @throws StoreException 
     */
    @Before
    public void before() {
        //create a PrimLongTreeMap
        map = new PrimLongTreeMap<String>();
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
        assertEquals("Check no element", false, map.containsValue(KEY1));
        assertEquals("Check no element", false, map.containsValue(ELEMENT4));
    }

//    /**
//     * Test the addAll methods add the correct values.
//     */
//    @Test
//    public void testPutAll() {
//        HashMap<Long, String> h= new HashMap<Long, String>();
//        h.put(KEY3, ELEMENT3);
//        h.put(KEY4, ELEMENT4);
//        h.put(KEY5, ELEMENT5);
//        h.put(KEY6, ELEMENT6);
//        
//        dbHashtable.putAll(h);
//        assertTrue("Check the number of values", dbHashtable.size()==6);
//        assertEquals("Check element 1", ELEMENT1, dbHashtable.get(KEY1));
//        assertEquals("Check element 2", ELEMENT2, dbHashtable.get(KEY2));
//        assertEquals("Check element 3", ELEMENT3, dbHashtable.get(KEY3));
//        assertEquals("Check element 4", ELEMENT4, dbHashtable.get(KEY4));
//        assertEquals("Check element 5", ELEMENT5, dbHashtable.get(KEY5));
//        assertEquals("Check element 6", ELEMENT6, dbHashtable.get(KEY6));
//    }

    /**
     * Test the keySet method returns the correct set.
     */
    @Test
    public void testKeySet() {
        SortedSet<Long> keys = new TreeSet<Long>(map.keySet());
        assertTrue("Check the number of keys", keys.size()==3);
        Long[] keyArray = keys.toArray(new Long[0]);
        assertEquals("Check key 1", KEY1, (long)keyArray[0]);
        assertEquals("Check key 2", KEY2, (long)keyArray[1]);
        assertEquals("Check key 3", KEY3, (long)keyArray[2]);
    }

    /**
     * Test the entrySet method returns the correct set.
     */
    @Test
    public void testEntrySet() {
        Set<PrimLongTreeMap.Entry<String>> entries = map.entrySet();
        assertTrue("Check the number of entries", entries.size()==3);
        boolean b1 = false;
        boolean b2 = false;
        boolean b3 = false;
        for (Iterator<PrimLongTreeMap.Entry<String>> i = 
            entries.iterator(); i.hasNext(); ) {
        	PrimLongTreeMap.Entry<String> me = i.next();
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
        
        assertFalse(entries.isEmpty());
        assertEquals(3, entries.size());
        Iterator<?> it1 = entries.iterator();
        it1.next();
        it1.remove();  //removes KEY1
        Object o2 = it1.next(); //key2
        Object o3 = it1.next(); //key3
        entries.remove(o3);
        assertEquals(1, entries.size());
        assertTrue(entries.contains(o2));
        entries.clear();
        assertTrue(entries.isEmpty());
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
    
    @Test
    public void testCeiling() {
    	assertEquals(KEY1, map.ceilingEntry(KEY0).getKey());
    	assertEquals(KEY1, (long)map.ceilingKey(KEY0));
    	assertEquals(KEY2, map.ceilingEntry(KEY2).getKey());
    	assertEquals(KEY2, (long)map.ceilingKey(KEY2));
    	assertEquals(null, map.ceilingEntry(KEY4));
    	assertEquals(null, map.ceilingKey(KEY4));
    }
    
    @Test
    public void testFloor() {
    	assertEquals(null, map.floorEntry(KEY0));
    	assertEquals(null, map.floorKey(KEY0));
    	assertEquals(KEY2, map.floorEntry(KEY2).getKey());
    	assertEquals(KEY2, (long)map.floorKey(KEY2));
    	assertEquals(KEY3, map.floorEntry(KEY4).getKey());
    	assertEquals(KEY3, (long)map.floorKey(KEY4));
    }
    
    @Test
    public void testLower() {
    	assertEquals(null, map.lowerEntry(KEY0));
    	assertEquals(null, map.lowerKey(KEY0));
    	assertEquals(null, map.lowerEntry(KEY1));
    	assertEquals(null, map.lowerKey(KEY1));
    	assertEquals(KEY1, map.lowerEntry(KEY2).getKey());
    	assertEquals(KEY1, (long)map.lowerKey(KEY2));
    	assertEquals(KEY3, map.lowerEntry(KEY4).getKey());
    	assertEquals(KEY3, (long)map.lowerKey(KEY4));
    }
    
    @Test
    public void testHigher() {
    	assertEquals(KEY1, map.higherEntry(KEY0).getKey());
    	assertEquals(KEY1, (long)map.higherKey(KEY0));
    	assertEquals(KEY2, map.higherEntry(KEY1).getKey());
    	assertEquals(KEY2, (long)map.higherKey(KEY1));
    	assertEquals(null, map.higherEntry(KEY3));
    	assertEquals(null, map.higherKey(KEY3));
    	assertEquals(null, map.higherEntry(KEY4));
    	assertEquals(null, map.higherKey(KEY4));
    }
    
    @Test
    public void testFirst() {
    	assertEquals(KEY1, map.firstEntry().getKey());
    	assertEquals(KEY1, (long)map.firstKey());
    }
    
    @Test
    public void testLast() {
    	assertEquals(KEY3, map.lastEntry().getKey());
    	assertEquals(KEY3, (long)map.lastKey());
    }
    
    
    @Test
    public void testPoll() {
    	assertEquals(KEY3, map.pollLastEntry().getKey());
    	assertEquals(KEY2, map.pollLastEntry().getKey());
    	assertEquals(KEY1, map.pollFirstEntry().getKey());
    	assertEquals(null, map.pollFirstEntry());
    }
    
    @Test
    public void testClone() {
    	@SuppressWarnings("unchecked")
		PrimLongTreeMap<String> m = (PrimLongTreeMap<String>) map.clone();
    	assertEquals(map.size(), m.size());
    }
    
}

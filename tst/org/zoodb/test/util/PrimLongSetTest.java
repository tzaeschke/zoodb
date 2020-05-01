/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zoodb.test.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.internal.util.PrimLongSet;
import org.zoodb.internal.util.PrimLongSetZ;

/**
 * Test harness for PrimLongSet.
 *
 * @author  Tilmann Zaeschke
 */
public class PrimLongSetTest {

    private static final long KEY1 = 1;
    private static final long KEY2 = 2;
    private static final long KEY3 = 3;
    private static final long KEY4 = 4;
    private static final long KEY5 = 5;
    private static final long KEY6 = 6;
    
    private PrimLongSet set;
    
    private PrimLongSet createMap() {
    	return new PrimLongSetZ();
    }
    
    /**
     * Run before each test.
     * The setUp method tests the put method of PrimLongTreeMap.
     */
    @Before
    public void before() {
        //create a PrimLongTreeSet
    	set = createMap(); 
        set.add(KEY1);
        set.add(KEY2);
        set.add(KEY3);
    }

    /**
     * Run after each test.
     */
    @After
    public void after() {
        set.clear();
    }

    /**
     * Test the clear() and size() methods.
     */
    @Test
    public void testClearAndSize() {
        assertEquals("Check size 1", 3, set.size());
        set.clear();
        assertEquals("Check size 2", 0, set.size());
    }
    
    /**
     * Test the values method returns the correct collection.
     */
    @Test
    public void testIterator() {
    	HashSet<Long> temp = new HashSet<Long>();
    	temp.add(KEY1);
    	temp.add(KEY2);
    	temp.add(KEY3);
        Iterator<Long> i = set.iterator();
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
        set.remove(KEY2);
        assertEquals("Check element 1", true, set.contains(KEY1));
        assertEquals("Check element 2", false, set.contains(KEY2));
        assertEquals("Check element 3", true, set.contains(KEY3));
        assertEquals("Check the number of values", 2, set.size());
    }

    /**
     * Test the contains method returns the correct results.
     */
    @Test
    public void testContains() {
        set.remove(KEY2);
        assertEquals("Check no element", true, set.contains(KEY1));
        assertEquals("Check no element", false, set.contains(KEY2));
        assertEquals("Check no element", false, set.contains(KEY4));
        assertEquals("Check no element", false, set.contains(KEY5));
    }

    /**
     * Test the addAll methods add the correct values.
     */
    @Test
    public void testPutAll() {
        PrimLongSet h = createMap();
        h.add(KEY3);
        h.add(KEY4);
        h.add(KEY5);
        h.add(KEY6);
        
        set.addAll(h);
        assertEquals("Check the number of values", 6, set.size());
        assertEquals("Check element 1", true, set.contains(KEY1));
        assertEquals("Check element 2", true, set.contains(KEY2));
        assertEquals("Check element 3", true, set.contains(KEY3));
        assertEquals("Check element 4", true, set.contains(KEY4));
        assertEquals("Check element 5", true, set.contains(KEY5));
        assertEquals("Check element 6", true, set.contains(KEY6));
    }

    /**
     * Test the entrySet method returns the correct set.
     */
    @Test
    public void testEntrySet() {
        Set<? extends PrimLongSetZ.Entry> entries = set.entries();
        assertTrue("Check the number of entries", entries.size()==3);
        boolean b1 = false;
        boolean b2 = false;
        boolean b3 = false;
        for (Iterator<? extends PrimLongSetZ.Entry> i = entries.iterator(); i.hasNext(); ) {
        	PrimLongSetZ.Entry me = i.next();
            if (me.getKey() == KEY1) {
                assertEquals("Check entry 1", KEY1, me.getKey());
                b1 = true;
            } else if (me.getKey() == KEY2) {
                assertEquals("Check entry 2", KEY2, me.getKey());
                b2 = true;
            } else if (me.getKey() == KEY3) {
                assertEquals("Check entry 3", KEY3, me.getKey());
                b3 = true;
            } else {
                fail ("Unexpected key found.");
            }
        }
        assertEquals("Check occurences of entries", b1 && b2 && b3, true);
    }
    
}

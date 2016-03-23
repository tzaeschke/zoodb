/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import org.zoodb.api.DBHashSet;
import org.zoodb.test.testutil.TestTools;

/**
 * Test harness for DBHashSet.
 *
 * @author  Tilmann Zaeschke
 */
public abstract class DBHashSetTest {

    private static final String DB_NAME = "TestDb";
    private static final String ELEMENT1 = "element one";
    private static final String ELEMENT2 = "second element";
    private static final String ELEMENT3 = "another element";
    private static final String ELEMENT4 = "fourth element";
    private static final Set<String> elementSet3;
    private static final Set<String> elementSet4;
    
    static {
    	elementSet3 = new HashSet<>();
    	elementSet3.add(ELEMENT1);
    	elementSet3.add(ELEMENT2);
    	elementSet3.add(ELEMENT3);
    	
    	elementSet4 = new HashSet<>();
    	elementSet4.add(ELEMENT1);
    	elementSet4.add(ELEMENT2);
    	elementSet4.add(ELEMENT3);
    	elementSet4.add(ELEMENT4);
    }
    
    public static final class Ctor1 extends DBHashSetTest {

		@Override
		protected DBHashSet<String> constructSet() {
			DBHashSet<String> set = new DBHashSet<String>();
			set.add(ELEMENT1);
			set.add(ELEMENT2);
			set.add(ELEMENT3);
			set.add(ELEMENT1);
			return set;
		}
    	
    }

    public static final class Ctor2 extends DBHashSetTest {

    	@Override
		protected DBHashSet<String> constructSet() {
    		DBHashSet<String> set = new DBHashSet<String>(50);
			set.add(ELEMENT1);
			set.add(ELEMENT2);
			set.add(ELEMENT3);
			set.add(ELEMENT1);
			return set;
		}
    	
    }

    public static final class Ctor3 extends DBHashSetTest {

		@Override
		protected DBHashSet<String> constructSet() {
			Collection<String> strCollection = new HashSet<>();
			strCollection.add(ELEMENT1);
			strCollection.add(ELEMENT2);
			strCollection.add(ELEMENT3);
			strCollection.add(ELEMENT1);
			return new DBHashSet<String>(strCollection);
		}
    	
    }
    
    private DBHashSet<String> _dbSet;
    
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
        _dbSet = this.constructSet();
    }
    
    protected abstract DBHashSet<String> constructSet();

    /**
     * Run after each test.
     */
    @After
    public void after() {
        _dbSet.clear();
    }

    /**
     * Test the add() and contains() methods.
     */
    @Test
    public void testAddAndContains() {
        assertFalse("Check contains before adding the element", _dbSet.contains(ELEMENT4));
        assertTrue("Check add return value (true)", _dbSet.add(ELEMENT4));
        assertFalse("Check add return value (false)", _dbSet.add(ELEMENT1));
        assertTrue("Check contains after adding the element", _dbSet.contains(ELEMENT4));
    }

    /**
     * Test the clear() and size() methods.
     */
    @Test
    public void testClearAndSize() {
        assertEquals("Check size 1", 3, _dbSet.size());
        _dbSet.clear();
        assertEquals("Check size 2", 0, _dbSet.size());
    }
    
    /**
     * Test the iterator method iterates over the correct collection.
     */
    @Test
    public void testIterator() {
        Iterator<String> i = _dbSet.iterator();
        try {
            HashSet<String> s = new HashSet<>(elementSet3);
            
            assertTrue("Check iterator element 1", s.remove(i.next()));
            assertTrue("Check iterator element 2", s.remove(i.next()));
            assertTrue("Check iterator element 3", s.remove(i.next()));
        } catch (NoSuchElementException e) {
            fail("This shouldn't happen since _dbHastable should contain " +
                    "three elements");
        }
        assertFalse("Check the number of remaining elements", i.hasNext());
    }

    /**
     * Test the remove method removes the correct values.
     */
    @Test
    public void testRemove() {
        assertTrue("Check contains before removing the element", _dbSet.contains(ELEMENT2));
        assertTrue("Check remove return value (true)", _dbSet.remove(ELEMENT2));
        assertFalse("Check remove return value (false)", _dbSet.remove(ELEMENT2));
        assertFalse("Check contains after removing the element", _dbSet.contains(ELEMENT2));
        assertTrue("Check the number of values", _dbSet.size() == 2);
    }

    /**
     * Test the addAll methods add the correct values.
     */

    @Test
    public void testAddAll() {
    	assertTrue("Check addAll return value (true)", _dbSet.addAll(elementSet4));
    	assertFalse("Check addAll return value (false)", _dbSet.addAll(elementSet4));
        
        assertTrue("Check the number of values", _dbSet.size() == 4);
        assertTrue("Check element 1", _dbSet.contains(ELEMENT1));
        assertTrue("Check element 2", _dbSet.contains(ELEMENT2));
        assertTrue("Check element 3", _dbSet.contains(ELEMENT3));
        assertTrue("Check element 4", _dbSet.contains(ELEMENT4));
    }

    /**
     * Test the values method returns the correct collection.
     */
    @Test
    public void testToArray() {
    	HashSet<String> set3 = new HashSet<>(elementSet3);
        Object[] arrayO =  _dbSet.toArray();
        assertTrue("Check the number of values", arrayO.length==3);
        assertTrue("Check value 1", set3.remove(arrayO[0]));
        assertTrue("Check value 2", set3.remove(arrayO[1]));
        assertTrue("Check value 3", set3.remove(arrayO[2]));

        set3 = new HashSet<>(elementSet3);
        String[] array = _dbSet.toArray(new String[0]);
        assertTrue("Check the number of values", array.length==3);
        assertTrue("Check value 1", set3.remove(array[0]));
        assertTrue("Check value 2", set3.remove(array[1]));
        assertTrue("Check value 3", set3.remove(array[2]));

        set3 = new HashSet<>(elementSet3);
        array = _dbSet.toArray(new String[10]);
        assertTrue("Check the number of values", array.length==10);
        assertTrue("Check value 1", set3.remove(array[0]));
        assertTrue("Check value 2", set3.remove(array[1]));
        assertTrue("Check value 3", set3.remove(array[2]));
        assertEquals("Check value 4", null, array[3]);
    }
    
}

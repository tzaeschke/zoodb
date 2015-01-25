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
package org.zoodb.test.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

import javax.jdo.JDOUserException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.IndexFactory;
import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.server.index.LongLongIndex.LongLongUIndex;
import org.zoodb.internal.util.CloseableIterator;
import org.zoodb.tools.ZooConfig;

public class TestLongLongUniqueIndex {

    /** Adjust this when adjusting page size! */
    private static final int MAX_DEPTH = 8;  //128
    //private static final int MAX_DEPTH = 4;  //1024
    private static final int PAGE_SIZE = 128;
    
    @BeforeClass
    public static void setUp() {
    	/** Adjust MAX_DEPTH accordingly! */
    	ZooConfig.setFilePageSize(PAGE_SIZE);
    }

    @AfterClass
    public static void tearDown() {
    	ZooConfig.setFilePageSize(ZooConfig.FILE_PAGE_SIZE_DEFAULT);
    }

    private StorageChannel createPageAccessFile() {
    	StorageChannel paf = new StorageRootInMemory(ZooConfig.getFilePageSize());
    	return paf;
    }
    
    private LongLongIndex.LongLongUIndex createIndex() {
        StorageChannel paf = createPageAccessFile();
        return createIndex(paf); 
    }
    
    private LongLongIndex.LongLongUIndex createIndex(StorageChannel paf) {
    	LongLongIndex.LongLongUIndex ind = IndexFactory.createUniqueIndex(DATA_TYPE.GENERIC_INDEX, paf);
    	return ind; 
    }
    
    
    @Test
    public void testAddStrongCheck() {
        final int MAX = 5000;
        LongLongIndex ind = createIndex();
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
            //Now check every entry!!!
            for (int j = 1000; j <= i; j++) {
                Iterator<LongLongIndex.LLEntry> llIter = ind.iterator(j, j);
                LongLongIndex.LLEntry e = llIter.next();
                assertNotNull(e);
                assertEquals(j, e.getKey());
                assertEquals(j+32, e.getValue());
                assertFalse(llIter.hasNext());
            }
        }
        System.out.println("Index size: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());

        assertFalse( ind.iterator(-1, -1).hasNext() );
        assertFalse( ind.iterator(0, 0).hasNext() );
        assertFalse( ind.iterator(999, 999).hasNext() );
        assertFalse( ind.iterator(1000 + MAX, 1000 + MAX).hasNext() );
    }

    @Test
    public void testAdd() {
        final int MAX = 1000000;
        LongLongIndex ind = createIndex();
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 30+i);  //will be overwritten
            ind.insertLong(i, 31+i);  //will be overwritten
            ind.insertLong(i, 32+i);
        }
        System.out.println("Index size: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());

        for (int i = 1000; i < 1000+MAX; i++) {
        	LongLongIndex.LLEntry e = ind.iterator(i, i).next();
            //			System.out.println(" Looking up: " + i);
            assertEquals( i, e.getKey() );
            assertEquals( 32+i, e.getValue() );
        }

        assertFalse( ind.iterator(-1, -1).hasNext() );
        assertFalse( ind.iterator(0, 0).hasNext() );
        assertFalse( ind.iterator(999, 999).hasNext() );
        assertFalse( ind.iterator(1000 + MAX, 1000 + MAX).hasNext() );

        System.out.println("inner: "+ ind.statsGetInnerN() + " outer: " + ind.statsGetLeavesN());
        double epp = MAX / ind.statsGetLeavesN();
        System.out.println("Entires per page: " + epp);
    }

    @Test
    public void testIterator() {
        final int MAX = 1000000;
        LongLongIndex ind = createIndex();

        Iterator<LongLongIndex.LLEntry> iter = ind.iterator();
        assertFalse(iter.hasNext());

        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
        }

        iter = ind.iterator();
        long prev = -1;
        int n = 0;
        while (iter.hasNext()) {
            long l = iter.next().getKey();
            assertTrue( l > prev );
            if (prev > 0) {
                assertEquals( prev+1, l );
            }
            prev = l;
            n++;
        }
        assertEquals(MAX, n);
    }

    @Test
    public void testInverseIterator() {
        final int MAX = 3000;
        LongLongIndex.LongLongUIndex ind = createIndex();
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
        }
        Iterator<LongLongIndex.LLEntry> iter = ind.descendingIterator();
        long prev = 1000+MAX;
        int n = MAX;
        while (iter.hasNext()) {
            long l = iter.next().getKey();
            assertTrue("l=" + l + " prev = "+ prev, l < prev );
            assertEquals("l=" + l + " prev = "+ (prev-1),  prev-1, l );
            prev = l;
            n--;
        }
        assertEquals(0, n);
    }


    @Test
    public void testDelete() {
        final int MAX = 1000000;
        LongLongIndex.LongLongUIndex ind = createIndex();
        //Fill index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
        }

        //		TreeSet<Long> toDelete = new TreeSet<Long>();
        //		Random rnd = new Random();
        //		for (int i = 0; i < MAX*10; i++) {
        //			toDelete.add( (long)rnd.nextInt(MAX)+1000 );
        //		}
        //TODO use the following after fixing the above
        Set<Long> toDelete = new LinkedHashSet<Long>();
        Random rnd = new Random();
        while (toDelete.size() < MAX*0.95) {
            toDelete.add( (long)rnd.nextInt(MAX)+1000 );
        }

        System.out.println("Index size before delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());
        int nIPagesBefore = ind.statsGetInnerN();
        int nLPagesBefore = ind.statsGetLeavesN();
        for (long l: toDelete) {
            ind.removeLong(l, -1);
        }
        System.out.println("Index size after delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());

        for (int i = 1000; i < 1000+MAX; i++) {
        	LongLongIndex.LLEntry fp = ind.findValue(i);
            if (toDelete.contains((long)i)) {
                assertNull(fp);
            } else {
                //			System.out.println(" Looking up: " + i);
                assertEquals( 32+i, fp.getValue() );
            }
        }

        //test iteration and size
        Iterator<LongLongIndex.LLEntry> iter = ind.iterator();
        long prev = -1;
        int n = 0;
        while (iter.hasNext()) {
            long l = iter.next().getKey();
            assertTrue( l > prev );
            assertFalse(toDelete.contains(l));
            prev = l;
            n++;
        }
        assertEquals(MAX-toDelete.size(), n);


        //Reduced inner pages
        assertTrue(nIPagesBefore >= ind.statsGetInnerN());
        //largely reduced lef pages
        assertTrue(nLPagesBefore + " -> " + ind.statsGetLeavesN(), 
                nLPagesBefore/2 > ind.statsGetLeavesN());
    }

    @Test
    public void testDeleteAll() {
        final int MAX = 1000000;
        LongLongIndex.LongLongUIndex ind = createIndex();

        //first a simple delete on empty index
        try {
            ind.removeLong(0, -1);
        	fail();
        } catch (NoSuchElementException e) {
        	//good!
        }

        //Fill index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, (32L<<32) + 32+i);
        }

        System.out.println("Index size before delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());
//        int nIPagesBefore = ind.statsGetInnerN();
        int nLPagesBefore = ind.statsGetLeavesN();

        //delete index
        for (int i = 1000; i < 1000+MAX; i++) {
            long prev = ind.removeLong(i, -1);
            assertEquals((32L<<32) + 32L + i, prev);
        }

        System.out.println("Index size after delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());
        for (int i = 1000; i < 1000+MAX; i++) {
            LongLongIndex.LLEntry e = ind.findValue(i);
            assertNull(e);
        }

        //test iteration and size
        Iterator<LongLongIndex.LLEntry> iter = ind.iterator();
        long prev = -1;
        int n = 0;
        while (iter.hasNext()) {
            long l = iter.next().getKey();
            assertTrue( l > prev );
            prev = l;
            n++;
        }
        assertEquals(0, n);


        //Reduced inner pages
        assertTrue(ind.statsGetInnerN() <= 1);
        //largely reduced leaf pages
        assertTrue(nLPagesBefore + " -> " + ind.statsGetLeavesN(), ind.statsGetLeavesN() <= 1);


        //and finally, try adding something again
        for (int i = 1000; i < 1000+1000; i++) {
            ind.insertLong(i, 32+i);
            //		System.out.println("Inserting: " + i);
            //Now check every entry!!!
            for (int j = 1000; j <= i; j++) {
                LongLongIndex.LLEntry e = ind.findValue(j);
                if (e == null) {
                    ind.print();
                    fail();
                }
            }
        }
    }

    /**
     * Test that only necessary pages get dirty.
     */
    @Test
    public void testDirtyPages() {
        //When increasing this number, also increase the assertion limit!
        final int MAX = 1000000;
        StorageChannel paf = createPageAccessFile();
        LongLongIndex ind = createIndex(paf);
        //Fill index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
        }

        //		int nW0 = paf.statsGetWriteCount();
        ind.write();
        int nW1 = paf.statsGetWriteCount();
        ind.insertLong(MAX * 2, 32);
        ind.write();
        int nW2 = paf.statsGetWriteCount();
        assertTrue("nW1="+nW1 + " / nW2="+nW2, nW2-nW1 <= MAX_DEPTH);


        ind.removeLong(MAX * 2, -1);
        ind.write();
        int nW3 = paf.statsGetWriteCount();
        assertTrue("nW2="+nW2 + " / nW3="+nW3, nW3-nW2 <= MAX_DEPTH);

        //TODO test more thoroughly?
    }

    @Test
    public void testMaxOid() {
        final int MAX = 1000000;
        LongLongIndex.LongLongUIndex ind = createIndex();
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
            assertEquals(i, ind.getMaxKey());
        }

        for (int i = 1000; i < 1000+MAX; i++) {
            LongLongIndex.LLEntry fp = ind.findValue(i);
            //			System.out.println(" Looking up: " + i);
            assertEquals( 32+i, fp.getValue() );
        }

        assertNull( ind.findValue(-1) );
        assertNull( ind.findValue(0) );
        assertNull( ind.findValue(999) );
        assertNull( ind.findValue(1000 + MAX) );
    }

    @Test
    public void testConcurrentModificationExceptionDescending() {
        LongLongIndex ind = createIndex();
        for (int i = 1000; i < 2000; i++) {
            ind.insertLong(i, 32);
        }

        //Iterate while deleting
        Iterator<LLEntry> iter = ind.descendingIterator();
        long l = iter.next().getKey();
        ind.removeLong(l, 32);
        try {
        	iter.hasNext();
        	fail();
        } catch (ConcurrentModificationException e) {
        	//good!
        }
        try {
        	iter.next();
        	fail();
        } catch (ConcurrentModificationException e) {
        	//good!
        }

        //try with updates  (updates existing entry)
        iter = ind.descendingIterator();
        long l2 = iter.next().getKey();
        ind.insertLong(l2, 2233);
        try {
        	iter.hasNext();
        	fail();
        } catch (ConcurrentModificationException e) {
        	//good!
        }
        try {
        	iter.next();
        	fail();
        } catch (ConcurrentModificationException e) {
        	//good!
        }

        //try with new entries
        iter = ind.descendingIterator();
        ind.insertLong(11, 2233);
        try {
        	iter.hasNext();
        	fail();
        } catch (ConcurrentModificationException e) {
        	//good!
        }
        try {
        	iter.next();
        	fail();
        } catch (ConcurrentModificationException e) {
        	//good!
        }
    }

    @Test
    public void testConcurrentModificationException() {
        LongLongIndex ind = createIndex();
        for (int i = 1000; i < 2000; i++) {
            ind.insertLong(i, 32);
        }

        //Iterate while deleting
        Iterator<LLEntry> iter = ind.iterator();
        long l = iter.next().getKey();
        ind.removeLong(l, 32);
        try {
        	iter.hasNext();
        	fail();
        } catch (ConcurrentModificationException e) {
        	//good!
        }
        try {
        	iter.next();
        	fail();
        } catch (ConcurrentModificationException e) {
        	//good!
        }

        //try with updates  (updates existing entry)
        iter = ind.iterator();
        long l2 = iter.next().getKey();
        ind.insertLong(l2, 2233);
        try {
        	iter.hasNext();
        	fail();
        } catch (ConcurrentModificationException e) {
        	//good!
        }
        try {
        	iter.next();
        	fail();
        } catch (ConcurrentModificationException e) {
        	//good!
        }

        //try with new entries
        iter = ind.iterator();
        ind.insertLong(11, 2233);
        try {
        	iter.hasNext();
        	fail();
        } catch (ConcurrentModificationException e) {
        	//good!
        }
        try {
        	iter.next();
        	fail();
        } catch (ConcurrentModificationException e) {
        	//good!
        }
    }

    @Test
    public void testTransactionContext() {
        StorageChannel paf = createPageAccessFile();
        LongLongIndex ind = createIndex(paf);
        for (int i = 1000; i < 2000; i++) {
            ind.insertLong(i, 32);
        }

        //Iterate while deleting
        Iterator<LLEntry> iter = ind.iterator();
        paf.newTransaction(22);
         try {
        	iter.hasNext();
        	fail();
        } catch (JDOUserException e) {
        	//good!
        }
        try {
        	iter.next();
        	fail();
        } catch (JDOUserException e) {
        	//good!
        }

        //try with updates  (updates existing entry)
        iter = ind.iterator();
        paf.newTransaction(33);
        try {
        	iter.hasNext();
        	fail();
        } catch (JDOUserException e) {
        	//good!
        }
        try {
        	iter.next();
        	fail();
        } catch (JDOUserException e) {
        	//good!
        }

        //try with new entries
        iter = ind.iterator();
        paf.newTransaction(44);
        try {
        	iter.hasNext();
        	fail();
        } catch (JDOUserException e) {
        	//good!
        }
        try {
        	iter.next();
        	fail();
        } catch (JDOUserException e) {
        	//good!
        }
    }

    @Test
    public void testTransactionContextDescending() {
        StorageChannel paf = createPageAccessFile();
        LongLongIndex ind = createIndex(paf);
        for (int i = 1000; i < 2000; i++) {
            ind.insertLong(i, 32);
        }

        //Iterate while deleting
        Iterator<LLEntry> iter = ind.descendingIterator();
        paf.newTransaction(22);
         try {
        	iter.hasNext();
        	fail();
        } catch (JDOUserException e) {
        	//good!
        }
        try {
        	iter.next();
        	fail();
        } catch (JDOUserException e) {
        	//good!
        }

        //try with updates  (updates existing entry)
        iter = ind.descendingIterator();
        paf.newTransaction(33);
        try {
        	iter.hasNext();
        	fail();
        } catch (JDOUserException e) {
        	//good!
        }
        try {
        	iter.next();
        	fail();
        } catch (JDOUserException e) {
        	//good!
        }

        //try with new entries
        iter = ind.descendingIterator();
        paf.newTransaction(44);
        try {
        	iter.hasNext();
        	fail();
        } catch (JDOUserException e) {
        	//good!
        }
        try {
        	iter.next();
        	fail();
        } catch (JDOUserException e) {
        	//good!
        }
    }

    @Test
    public void testSpaceUsage() {
        final int MAX = 1000000;
        LongLongIndex ind = createIndex();
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
        }

        
        System.out.println("inner: "+ ind.statsGetInnerN() + " outer: " + ind.statsGetLeavesN());
        double epp = MAX / ind.statsGetLeavesN();
        System.out.println("Entries per page: " + epp);
        assertTrue(epp >= PAGE_SIZE/32);
        double lpi = (ind.statsGetLeavesN() + ind.statsGetInnerN()) / ind.statsGetInnerN();
        System.out.println("Leaves per inner page: " + lpi);
        assertTrue(lpi >= PAGE_SIZE/32);
    }

    @Test
    public void testSpaceUsageReverseInsert() {
        final int MAX = 1000000;
        LongLongIndex ind = createIndex();
        for (int i = 1000; i < 2000; i++) {
            ind.insertLong(i, 32+i);
        }
        for (int i = 1000+MAX-1; i >= 2000; i--) {
            ind.insertLong(i, 32+i);
        }

        System.out.println("inner: "+ ind.statsGetInnerN() + " outer: " + ind.statsGetLeavesN());
        double epp = MAX / ind.statsGetLeavesN();
        System.out.println("Entries per page: " + epp + "/" + PAGE_SIZE/32);
        assertTrue(epp >= PAGE_SIZE/32);
        double lpi = (ind.statsGetLeavesN() + ind.statsGetInnerN()) / ind.statsGetInnerN();
        System.out.println("Leaves per inner page: " + lpi);
        assertTrue(lpi >= PAGE_SIZE/32);
    }


    @Test
    public void testLoadedPagesNotDirty() {
        final int MAX = 1000000;
        StorageChannel paf = createPageAccessFile();
        LongLongUIndex ind = IndexFactory.createUniqueIndex(DATA_TYPE.GENERIC_INDEX, paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
        }
        int root = ind.write();
//        int w0 = ind.statsGetWrittenPagesN();
//        System.out.println("w0=" + w0);

        //now read it
        LongLongUIndex ind2 = IndexFactory.loadUniqueIndex(DATA_TYPE.GENERIC_INDEX, paf, root);
        int w1 = ind2.statsGetWrittenPagesN();
        Iterator<LongLongIndex.LLEntry> i = ind2.iterator(Long.MIN_VALUE, Long.MAX_VALUE);
        int n = 0;
        while (i.hasNext()) {
        	n++;
        	i.next();
        }
        ind2.write();
        int w2 = ind2.statsGetWrittenPagesN();
        //no pages written on freshly read root
        assertEquals("w1=" + w1, 0, w1);
        //no pages written when only reading
        assertEquals("w1=" + w1 + "  w2=" + w2, w1, w2);
                
        //now add one element and see how much gets written
//        ind2.insertLong(-1, -1);
//        assertNotNull(ind2.findValue(-1));
//        ind2.insertLong(11, 11);
        ind2.insertLong(1100, 1100);
//        LLEntry e = ind2.findValue(1100);
//        assertNotNull(e);
//        assertEquals(1100, e.getValue());
        ind2.write();
        int wn = ind2.statsGetWrittenPagesN();
//        System.out.println("w2=" + w2);
//        System.out.println("wn=" + wn);
        assertTrue("wn=" + wn, wn > w2);
        assertTrue("wn=" + wn, wn <= MAX_DEPTH);
        
        assertEquals(MAX, n);
    }

    
    @Test
    public void testWriting() {
        final int MAX = 1000000;
        StorageChannel paf = createPageAccessFile();
        LongLongUIndex ind = IndexFactory.createUniqueIndex(DATA_TYPE.GENERIC_INDEX, paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
        }
        int root = ind.write();

        //now read it
        LongLongUIndex ind2 = IndexFactory.loadUniqueIndex(DATA_TYPE.GENERIC_INDEX, paf, root);
        Iterator<LongLongIndex.LLEntry> i = ind2.iterator(Long.MIN_VALUE, Long.MAX_VALUE);
        int n = 0;
        while (i.hasNext()) {
        	n++;
        	i.next();
        }
        
        assertEquals(MAX, n);
    }

    
    @Test
    public void testAddOverwrite() {
        final int MAX = 1000000;
        LongLongIndex.LongLongUIndex ind = createIndex();
        
        // fill index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, i);
        }

        // overwrite with same values
        for (int i = 1000; i < 1000+MAX; i++) {
        	ind.insertLong(i, i);
        }

        //check element count
        Iterator<LongLongIndex.LLEntry> it = ind.iterator();
        int n = 0;
        while (it.hasNext()) {
        	LongLongIndex.LLEntry e = it.next();
        	assertEquals(n+1000, e.getKey());
        	assertEquals(n+1000, e.getValue());
        	n++;
        }
        assertEquals(MAX, n);
    	
        // overwrite with different values
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, i+1);
        }

        //check element count
        it = ind.iterator();
        n = 0;
        while (it.hasNext()) {
        	LongLongIndex.LLEntry e = it.next();
        	assertEquals(n+1000, e.getKey());
        	assertEquals(n+1+1000, e.getValue());
        	n++;
        }
        assertEquals(MAX, n);
    }

    
    @Test
    public void testMax() {
        LongLongIndex.LongLongUIndex ind = createIndex();
        
        assertEquals(Long.MIN_VALUE, ind.getMaxKey());
        
        ind.insertLong(123, 456);
        assertEquals(123, ind.getMaxKey());

        ind.insertLong(1235, 456);
        assertEquals(1235, ind.getMaxKey());

        ind.insertLong(-1235, 456);
        assertEquals(1235, ind.getMaxKey());

        ind.removeLong(123);
        assertEquals(1235, ind.getMaxKey());

        ind.removeLong(1235);
        assertEquals(-1235, ind.getMaxKey());

        ind.removeLong(-1235);
        assertEquals(Long.MIN_VALUE, ind.getMaxKey());
    }
    
    
    @Test
    public void testClear() {
        LongLongIndex.LongLongUIndex ind = createIndex();

        CloseableIterator<?> it0 = ind.iterator(Long.MIN_VALUE, Long.MAX_VALUE);
        assertFalse(it0.hasNext());
        it0.close();

        int MAX = 100000;
        for (int j = 0; j < 3; j++) {
	        for (int i = 0; i < MAX; i++) {
	        	ind.insertLong(MAX, i*2);
	        }
	        ind.clear();
	        for (int i = 0; i < MAX; i++) {
	        	//TODO assert
	        	ind.findValue(i);
	        }
	        CloseableIterator<?> it1 = ind.iterator(Long.MIN_VALUE, Long.MAX_VALUE);
	        assertFalse(it1.hasNext());
	        it1.close();
	        CloseableIterator<?> it2 = ind.iterator(1, 1000);
	        assertFalse(it2.hasNext());
	        it2.close();
	        assertEquals(Long.MIN_VALUE, ind.getMaxKey());
        }
        
    }

    
    @Test
    public void testStats() {
        final int MAX = 1000000;
        LongLongUIndex ind = createIndex();

        assertEquals(1, ind.statsGetInnerN());
        assertEquals(0, ind.statsGetLeavesN());
        
        //Fill index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
        }

        System.out.println("Index size before delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());
        //int nIPagesBefore = ind.statsGetInnerN();
        int nLPagesBefore = ind.statsGetLeavesN();

        assertTrue("nInner="+ ind.statsGetInnerN(), 10000 < ind.statsGetInnerN());
        assertTrue("nLeaves="+ ind.statsGetLeavesN(), 100000 < ind.statsGetLeavesN());

        
        //delete index
        for (int i = 1000; i < 1000+MAX; i++) {
        	long prev = ind.removeLong(i, 32+i);
            assertEquals(i + 32, prev);
        }

        System.out.println("Index size after delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());

        assertEquals(1, ind.statsGetInnerN());
        assertEquals(0, ind.statsGetLeavesN());


        //Reduced inner pages
        assertTrue(ind.statsGetInnerN() <= 1);
        //largely reduced leaf pages
        assertTrue(nLPagesBefore + " -> " + ind.statsGetLeavesN(), ind.statsGetLeavesN() <= 1);
    }


    //TODO test random add
    //TODO test values/pages > 63bit/31bit (MAX_VALUE?!)
    //TODO test iterator with random add


}

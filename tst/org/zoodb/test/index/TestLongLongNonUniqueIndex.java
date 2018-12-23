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
package org.zoodb.test.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.TreeMap;

import javax.jdo.JDOUserException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.internal.server.DiskAccess;
import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.IOResourceProvider;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.IndexFactory;
import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.util.CloseableIterator;
import org.zoodb.tools.ZooConfig;

public class TestLongLongNonUniqueIndex {

    /** Adjust this when adjusting page size! */
	private static final int MAX_DEPTH = 13;  //128
	//private static final int MAX_DEPTH = 8;  //128  TODO see index improvements in TODO.txt
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

    @Before
    public void setUpTest() {
    	//For tests after testDeleteWithMock() 
    	ZooConfig.setFilePageSize(PAGE_SIZE);
    }
    
    private IOResourceProvider createPageAccessFile() {
    	return new StorageRootInMemory(ZooConfig.getFilePageSize(), DiskAccess.NULL).createChannel(DiskAccess.NULL);
    }
    
    private LongLongIndex createIndex() {
    	IOResourceProvider paf = createPageAccessFile();
        return createIndex(paf); 
    }
    
    private LongLongIndex createIndex(IOResourceProvider paf) {
    	return IndexFactory.createIndex(PAGE_TYPE.GENERIC_INDEX, paf);
    }
    
    @Test
    public void testAddWithMockStrongCheck() {
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
        //System.out.println("Index size: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
        //        ind.statsGetLeavesN());

        assertFalse( ind.iterator(-1, -1).hasNext() );
        assertFalse( ind.iterator(0, 0).hasNext() );
        assertFalse( ind.iterator(999, 999).hasNext() );
        assertFalse( ind.iterator(1000 + MAX, 1000 + MAX).hasNext() );
    }

    
    @Test
    public void testAddWithMock() {
        final int MAX = 1000000;
        LongLongIndex ind = createIndex();
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 2+i);
            ind.insertLong(i, 3+i);
            ind.insertLong(i, 1+i);
        }
        //System.out.println("Index size: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
        //        ind.statsGetLeavesN());

        for (int i = 1000; i < 1000+MAX; i++) {
        	Iterator<LongLongIndex.LLEntry> llIter = ind.iterator(i, i);
        	LongLongIndex.LLEntry e = llIter.next();
            assertEquals( i, e.getKey());
            assertEquals( 1+i, e.getValue());
            e = llIter.next();
            assertEquals( i, e.getKey());
            assertEquals( 2+i, e.getValue());
            e = llIter.next();
            assertEquals( i, e.getKey());
            assertEquals( 3+i, e.getValue());
            assertFalse(llIter.hasNext());
        }

        assertFalse( ind.iterator(-1, -1).hasNext() );
        assertFalse( ind.iterator(0, 0).hasNext() );
        assertFalse( ind.iterator(999, 999).hasNext() );
        assertFalse( ind.iterator(1000 + MAX, 1000 + MAX).hasNext() );
    }

    
    @Test
    public void testAddWithMockReverse() {
        final int MAX = 1000000;
        LongLongIndex ind = createIndex();
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 2+i);
            ind.insertLong(i, 3+i);
            ind.insertLong(i, 1+i);
        }
        //System.out.println("Index size: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
        //        ind.statsGetLeavesN());

        for (int i = 1000; i < 1000+MAX; i++) {
        	Iterator<LongLongIndex.LLEntry> llIter = ind.descendingIterator(i, i);
        	LongLongIndex.LLEntry e = llIter.next();
            assertEquals( i, e.getKey());
            assertEquals( 3+i, e.getValue());
            e = llIter.next();
            assertEquals( i, e.getKey());
            assertEquals( 2+i, e.getValue());
            e = llIter.next();
            assertEquals( i, e.getKey());
            assertEquals( 1+i, e.getValue());
            assertFalse(llIter.hasNext());
        }

        assertFalse( ind.iterator(-1, -1).hasNext() );
        assertFalse( ind.iterator(0, 0).hasNext() );
        assertFalse( ind.iterator(999, 999).hasNext() );
        assertFalse( ind.iterator(1000 + MAX, 1000 + MAX).hasNext() );
    }

    
    @Test
    public void testIteratorWithMock() {
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
    public void testInverseIteratorWithMock() {
    	//1.000.000
        final int MAX = 1000000;
        LongLongIndex ind = createIndex();
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32);
            ind.insertLong(i, 11);
            ind.insertLong(i, 33);
        }

        Iterator<LongLongIndex.LLEntry> iter = ind.descendingIterator();
        long prevKey = 1000+MAX;
        long prevVal = 11;
        int n = MAX;
        while (iter.hasNext()) {
        	LongLongIndex.LLEntry e = iter.next();
            long k = e.getKey();
            long v = e.getValue();
            if ( k < prevKey) {
	            assertEquals( prevKey-1, k );
	            assertEquals( 11, prevVal );
	            assertEquals( 33, v );
	            prevKey = k;
	            n--;
            } else {
            	if (prevVal == 33) {
            		assertEquals( v, 32 );
            	} else if (prevVal == 32) {
            		assertEquals( v, 11 );
            	} else {
            		fail();
            	}
            }
            prevVal = v;
        }
        assertEquals(0, n);
    }


    @Test
    public void testDeleteWithMock() {
    	ZooConfig.setFilePageSize(1024);
        final int MAX = 1000000;
        LongLongIndex ind = createIndex();
        TreeMap<Long, Long> toDelete = new TreeMap<Long, Long>();
        Random rnd = new Random();

        //Fill index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
            //delete ~75%
            if (rnd.nextBoolean() || rnd.nextBoolean()) {
            	toDelete.put((long)i, (long)32+i);
            }
        }

        //System.out.println("Index size before delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
        //        ind.statsGetLeavesN());
        int nIPagesBefore = ind.statsGetInnerN();
        int nLPagesBefore = ind.statsGetLeavesN();
        for (Map.Entry<Long, Long> e: toDelete.entrySet()) {
            ind.removeLong(e.getKey(), e.getValue());
        }
        //System.out.println("Index size after delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
        //        ind.statsGetLeavesN());

        for (int i = 1000; i < 1000+MAX; i++) {
            Iterator<LongLongIndex.LLEntry> ei = ind.iterator(i, i);
            if (toDelete.containsKey((long)i)) {
                assertFalse(ei.hasNext());
            } else {
                //			System.out.println(" Looking up: " + i);
                assertEquals( 32+i, ei.next().getValue() );
            }
        }

        //test iteration and size
        Iterator<LongLongIndex.LLEntry> iter = ind.iterator();
        long prev = -1;
        int n = 0;
        while (iter.hasNext()) {
            long l = iter.next().getKey();
            assertTrue( l > prev );
            assertFalse(toDelete.containsKey(l));
            prev = l;
            n++;
        }
        assertEquals(MAX-toDelete.size(), n);

        //Reduced inner pages
        assertTrue(nIPagesBefore >= ind.statsGetInnerN());
        //largely reduced lef pages
        //TODO fix this, see MAX_DEPTH and index improvements in TODO.txt (page fill rate)
        //This test behaves so bad for small pages, because page merge is only allowed for multiples
        //of 8. Should we instead check for nEntries==MAx>>1 then == (MAX>>2) then <= (MAX>>3)?
//        assertTrue(nLPagesBefore + " -> " + ind.statsGetLeavesN(), 
//        		nLPagesBefore/2 > ind.statsGetLeavesN());
        
//        System.out.println(nLPagesBefore + " -> " + ind.statsGetLeavesN() + 
//        		" should be " + nLPagesBefore*0.5);
        assertTrue(nLPagesBefore + " -> " + ind.statsGetLeavesN() + 
        		" should be " + nLPagesBefore*0.5, 
        		nLPagesBefore*0.95 > ind.statsGetLeavesN());
    	ZooConfig.setFilePageSize(PAGE_SIZE);
    }

    @Test
    public void testDeleteAllWithMock() {
        final int MAX = 1000000;
        LongLongIndex ind = createIndex();

        //first a simple delete on empty index
        try {
        	ind.removeLong(0, 0);
        	fail();
        } catch (NoSuchElementException e) {
        	//good!
        }

        //Fill index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
        }

        //System.out.println("Index size before delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
        //        ind.statsGetLeavesN());
        //int nIPagesBefore = ind.statsGetInnerN();
        int nLPagesBefore = ind.statsGetLeavesN();

        //delete index
        for (int i = 1000; i < 1000+MAX; i++) {
        	long prev = ind.removeLong(i, 32+i);
            assertEquals(i + 32, prev);
        }

        //System.out.println("Index size after delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
        //        ind.statsGetLeavesN());
        for (int i = 1000; i < 1000+MAX; i++) {
            Iterator<LongLongIndex.LLEntry> ie = ind.iterator(i, i);
            assertFalse(ie.hasNext());
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
                Iterator<LongLongIndex.LLEntry> fp2 = ind.iterator(j, j);
                if (!fp2.hasNext()) {
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
    public void testDirtyPagesWithMock() {
        //When increasing this number, also increase the assertion limit!
        final int MAX = 1000000;
        IOResourceProvider paf = createPageAccessFile();
        LongLongIndex ind = createIndex(paf);
        
        //Fill index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
        }

        //		int nW0 = paf.statsGetWriteCount();
        paf.writeIndex(ind::write);
        int nW1 = paf.statsGetWriteCount();
        ind.insertLong(MAX * 2, 32);
        paf.writeIndex(ind::write);
        int nW2 = paf.statsGetWriteCount();
        assertTrue("nW1="+nW1 + " / nW2="+nW2, nW2-nW1 <= MAX_DEPTH);


        ind.removeLong(MAX * 2, 32);
        paf.writeIndex(ind::write);
        int nW3 = paf.statsGetWriteCount();
        assertTrue("nW2="+nW2 + " / nW3="+nW3, nW3-nW2 <= MAX_DEPTH);

        //TODO test more thoroughly?
    }

    @Test
    public void testMaxOidWithMock() {
        final int MAX = 1000000;
        LongLongIndex ind = createIndex();
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
            assertEquals(i, ind.getMaxKey());
        }

        for (int i = 1000; i < 1000+MAX; i++) {
            LongLongIndex.LLEntry fp = ind.iterator(i, i).next();
            //			System.out.println(" Looking up: " + i);
            assertEquals( 32+i, fp.getValue() );
        }

        assertFalse( ind.iterator(-1, -1).hasNext() );
        assertFalse( ind.iterator(0, 0).hasNext() );
        assertFalse( ind.iterator(999, 999).hasNext() );
        assertFalse( ind.iterator(1000 + MAX, 1000 + MAX).hasNext() );
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
        IOResourceProvider paf = createPageAccessFile();
        LongLongIndex ind = createIndex(paf);
        for (int i = 1000; i < 2000; i++) {
            ind.insertLong(i, 32);
        }

        //Iterate while deleting
        Iterator<LLEntry> iter = ind.iterator();
        paf.startWriting(22);
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
        paf.startWriting(33);
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
        paf.startWriting(44);
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
    	IOResourceProvider paf = createPageAccessFile();
        LongLongIndex ind = createIndex(paf);
        for (int i = 1000; i < 2000; i++) {
            ind.insertLong(i, 32);
        }

        //Iterate while deleting
        Iterator<LLEntry> iter = ind.descendingIterator();
        paf.startWriting(22);
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
        paf.startWriting(33);
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
        paf.startWriting(44);
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

    /**
     * Test adding lots of same-key entries. 
     */
    @Test
    public void testAddManyEqualWithMockStrong() {
        final int MAX = 5000;
        LongLongIndex ind = createIndex();
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(32, i);
            ind.insertLong(11, i);
            ind.insertLong(33, i);

        	Iterator<LongLongIndex.LLEntry> iter = ind.iterator(11, 11);
        	for (int ii = 1000; ii <= i; ii++) {
        		LongLongIndex.LLEntry e = iter.next();
                assertEquals( 11, e.getKey());
                assertEquals( ii, e.getValue());
            }
        	assertFalse(iter.hasNext());

        	iter = ind.iterator(33, 33);
        	for (int ii = 1000; ii <= i; ii++) {
        		LongLongIndex.LLEntry e = iter.next();
                assertEquals( 33, e.getKey());
                assertEquals( ii, e.getValue());
            }
        	assertFalse(iter.hasNext());

        	iter = ind.iterator(32, 32);
        	for (int ii = 1000; ii <= i; ii++) {
        		LongLongIndex.LLEntry e = iter.next();
                assertEquals( 32, e.getKey());
                assertEquals( ii, e.getValue());
            }
        	assertFalse(iter.hasNext());

        	//count ascending
        	iter = ind.iterator();
        	int n = 0;
        	while (iter.hasNext()) {
        		iter.next();
        		n++;
        	}
        	assertFalse(iter.hasNext());
            assertEquals((i-1000+1)*3, n);

            //count descending
            iter = ind.descendingIterator();
        	n = 0;
        	while (iter.hasNext()) {
        		iter.next();
        		n++;
        	}
        	assertFalse(iter.hasNext());
            assertEquals((i-1000+1)*3, n);
        }
        //System.out.println("Index size: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
        //        ind.statsGetLeavesN());

        assertFalse( ind.iterator(-1, -1).hasNext() );
        assertFalse( ind.iterator(0, 0).hasNext() );
        assertFalse( ind.iterator(999, 999).hasNext() );
        assertFalse( ind.iterator(1000 + MAX, 1000 + MAX).hasNext() );
    }

    
    /**
     * Test adding lots of same-key entries. 
     */
    @Test
    public void testAddManyEqualWithMock() {
        final int MAX = 1000000;
        LongLongIndex ind = createIndex();
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(32, i);
            ind.insertLong(11, i);
            ind.insertLong(33, i);
        }
        //System.out.println("Index size: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
        //        ind.statsGetLeavesN());

    	Iterator<LongLongIndex.LLEntry> iter = ind.iterator(11, 11);
    	for (int i = 1000; i < 1000+MAX; i++) {
    		LongLongIndex.LLEntry e = iter.next();
            assertEquals( 11, e.getKey());
            assertEquals( i, e.getValue());
        }
    	assertFalse(iter.hasNext());

    	iter = ind.iterator(33, 33);
    	for (int i = 1000; i < 1000+MAX; i++) {
    		LongLongIndex.LLEntry e = iter.next();
            assertEquals( 33, e.getKey());
            assertEquals( i, e.getValue());
        }
    	assertFalse(iter.hasNext());

    	iter = ind.iterator(32, 32);
    	for (int i = 1000; i < 1000+MAX; i++) {
    		LongLongIndex.LLEntry e = iter.next();
            assertEquals( 32, e.getKey());
            assertEquals( i, e.getValue());
        }
    	assertFalse(iter.hasNext());

    	//count ascending
    	iter = ind.iterator();
    	int n = 0;
    	while (iter.hasNext()) {
    		iter.next();
    		n++;
    	}
    	assertFalse(iter.hasNext());
        assertEquals(MAX*3, n);

        //count descending
        iter = ind.descendingIterator();
    	n = 0;
    	while (iter.hasNext()) {
    		iter.next();
    		n++;
    	}
    	assertFalse(iter.hasNext());
        assertEquals(MAX*3, n);

        assertFalse( ind.iterator(-1, -1).hasNext() );
        assertFalse( ind.iterator(0, 0).hasNext() );
        assertFalse( ind.iterator(999, 999).hasNext() );
        assertFalse( ind.iterator(1000 + MAX, 1000 + MAX).hasNext() );
    }

    
    @Test
    public void testManyEqualWithMock() {
        final int MAX = 1000000;
        final int VAR = 10;
        LongLongIndex ind = createIndex();
        Random rnd = new Random();
        int[] varCnt = new int[VAR];
        long sum = 0;
        
        // fill index
        for (int i = 1000; i < 1000+MAX; i++) {
        	int r = Math.abs(rnd.nextInt() % VAR);
            ind.insertLong(r, i);
            varCnt[r]++;
            sum += i;
        }

        //compare
        LongLongIndex.LongLongIterator<LongLongIndex.LLEntry> it = ind.iterator();
        int[] varCnt2 = new int[VAR];
        long sum2 = 0;
        while (it.hasNext()) {
        	LongLongIndex.LLEntry ie = it.next();
        	varCnt2[(int)ie.getKey()]++;
        	sum2 += ie.getValue();
        }
        for (int i = 0; i < VAR; i++) {
        	assertEquals(varCnt[i], varCnt2[i]);
        }
        assertEquals(sum, sum2);
        
        
    	//remove some stuff
        ind.iterator();
        while (it.hasNext()) {
        	LongLongIndex.LLEntry ie = it.next();
        	if (ie.getValue() % 2 == 0) {
                ind.removeLong(ie.getKey(), ie.getValue());
                varCnt[(int)ie.getKey()]--;
                sum2 -= ie.getValue();
        	}
        }
    	

        //compare again
        it = ind.iterator();
        varCnt2 = new int[VAR];
        sum2 = 0;
        while (it.hasNext()) {
        	LongLongIndex.LLEntry ie = it.next();
        	varCnt2[(int)ie.getKey()]++;
        	sum2 += ie.getValue();
        }
        for (int i = 0; i < VAR; i++) {
        	assertEquals(varCnt[i], varCnt2[i]);
        }
        assertEquals(sum, sum2);
    }

    
    @Test
    public void testAddOverwrite() {
        final int MAX = 1000000;
        LongLongIndex ind = createIndex();
        
        // fill index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, i);
        }

        // overwrite with same values
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, i);
        }

        //check element count
        LongLongIndex.LongLongIterator<LongLongIndex.LLEntry> it = ind.iterator();
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

        //check element count again, should have doubled
        it = ind.iterator();
        n = 0;
        int i = 0;
        while (it.hasNext()) {
        	//first entry
        	LongLongIndex.LLEntry e = it.next();
        	assertEquals(i+1000, e.getKey());
        	assertEquals(i+1000, e.getValue());
        	n++;
        	
        	//2nd entry
        	e = it.next();
        	assertEquals(i+1000, e.getKey());
        	assertEquals(i+1+1000, e.getValue());
        	n++;
        	i++;
        }
        assertEquals(MAX*2, n);
    }

    
    @Test
    public void testEmpty() {
        final int MAX = 30000;
        LongLongIndex ind = createIndex();

        ind.print();
        
        //check element count
        CloseableIterator<LongLongIndex.LLEntry> it = ind.iterator(1, Long.MAX_VALUE);
        assertFalse(it.hasNext());
        CloseableIterator<LongLongIndex.LLEntry> it2 = ind.descendingIterator();
        assertFalse(it2.hasNext());

    
        // fill index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, i);
        }
        // empty index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.removeLong(i, i);
        }
        ind.print();
        it = ind.iterator(1, Long.MAX_VALUE);
        assertFalse(it.hasNext());
        it2 = ind.descendingIterator();
        assertFalse(it2.hasNext());
    }

    @Test
    public void testSpaceUsageKey() {
        final int MAX = 1000000;
        LongLongIndex ind = createIndex();
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
        }
        
        //System.out.println("inner: "+ ind.statsGetInnerN() + " outer: " + ind.statsGetLeavesN());
        double epp = MAX / ind.statsGetLeavesN();
        //System.out.println("Entries per page: " + epp);
        assertTrue("epp=" + epp, epp >= PAGE_SIZE/32);  //   1/(8byte + 8 byte)/2  -> 2 for min half fill grade 
        double lpi = (ind.statsGetLeavesN() + ind.statsGetInnerN()) / ind.statsGetInnerN();
        //System.out.println("Leaves per inner page: " + lpi);
        assertTrue(lpi >= PAGE_SIZE/48);
    }

    @Test
    public void testSpaceUsageValue() {
        final int MAX = 1000000;
        LongLongIndex ind = createIndex();
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(32, 32+i);
        }
        
        //System.out.println("inner: "+ ind.statsGetInnerN() + " outer: " + ind.statsGetLeavesN());
        double epp = MAX / ind.statsGetLeavesN();
        //System.out.println("Entries per page: " + epp);
        assertTrue("epp=" + epp, epp >= PAGE_SIZE/32);
        double lpi = (ind.statsGetLeavesN() + ind.statsGetInnerN()) / ind.statsGetInnerN();
        //System.out.println("Leaves per inner page: " + lpi);
        assertTrue(lpi >= PAGE_SIZE/48);
    }

    @Test
    public void testSpaceUsageReverseInsertKeys() {
        final int MAX = 1000000;
        LongLongIndex ind = createIndex();
        for (int i = 1000; i < 2000; i++) {
            ind.insertLong(i, 32);
        }
        for (int i = 1000+MAX-1; i >= 2000; i--) {
            ind.insertLong(i, 32);
        }

        //System.out.println("inner: "+ ind.statsGetInnerN() + " outer: " + ind.statsGetLeavesN());
        double epp = MAX / ind.statsGetLeavesN();
        //System.out.println("Entries per page: " + epp + "/" + PAGE_SIZE/32);
        assertTrue("epp=" + epp, epp >= PAGE_SIZE/32);
        double lpi = (ind.statsGetLeavesN() + ind.statsGetInnerN()) / ind.statsGetInnerN();
        //System.out.println("Leaves per inner page: " + lpi);
        assertTrue(lpi >= PAGE_SIZE/48);
    }


    @Test
    public void testSpaceUsageReverseInsertValues() {
        final int MAX = 1000000;
        LongLongIndex ind = createIndex();
        for (int i = 1000; i < 2000; i++) {
            ind.insertLong(32, i);
        }
        for (int i = 1000+MAX-1; i >= 2000; i--) {
            ind.insertLong(32, i);
        }

        //System.out.println("inner: "+ ind.statsGetInnerN() + " outer: " + ind.statsGetLeavesN());
        double epp = MAX / ind.statsGetLeavesN();
        //System.out.println("Entries per page: " + epp + "/" + PAGE_SIZE/32);
        assertTrue("epp=" + epp, epp >= PAGE_SIZE/32);
        double lpi = (ind.statsGetLeavesN() + ind.statsGetInnerN()) / ind.statsGetInnerN();
        //System.out.println("Leaves per inner page: " + lpi);
        assertTrue(lpi >= PAGE_SIZE/48);
    }
    
    
    @Test
    public void testClear() {
    	LongLongIndex ind = createIndex();

        int MAX = 100000;
        for (int j = 0; j < 3; j++) {
	        for (int i = 0; i < MAX; i++) {
	        	ind.insertLong(MAX, i*2);
	        }
	        ind.clear();
	        for (int i = 0; i < MAX; i++) {
	        	assertFalse(ind.iterator(i, i).hasNext());
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
        LongLongIndex ind = createIndex();

        assertEquals(1, ind.statsGetInnerN());
        assertEquals(0, ind.statsGetLeavesN());
        
        //Fill index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
        }

        //System.out.println("Index size before delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
        //        ind.statsGetLeavesN());
        //int nIPagesBefore = ind.statsGetInnerN();
        int nLPagesBefore = ind.statsGetLeavesN();

        assertTrue("nInner="+ ind.statsGetInnerN(), 10000 < ind.statsGetInnerN());
        assertTrue("nLeaves="+ ind.statsGetLeavesN(), 100000 < ind.statsGetLeavesN());

        
        //delete index
        for (int i = 1000; i < 1000+MAX; i++) {
        	long prev = ind.removeLong(i, 32+i);
            assertEquals(i + 32, prev);
        }

        //System.out.println("Index size after delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
        //        ind.statsGetLeavesN());

        assertEquals(1, ind.statsGetInnerN());
        assertEquals(0, ind.statsGetLeavesN());


        //Reduced inner pages
        assertTrue(ind.statsGetInnerN() <= 1);
        //largely reduced leaf pages
        assertTrue(nLPagesBefore + " -> " + ind.statsGetLeavesN(), ind.statsGetLeavesN() <= 1);
    }



    //TODO test random add
    //TODO test overwrite
    //TODO test values/pages > 63bit/31bit (MAX_VALUE?!)
    //TODO test iterator with random add


}

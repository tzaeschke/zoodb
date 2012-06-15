/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.test.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.api.ZooConfig;
import org.zoodb.jdo.internal.server.StorageChannel;
import org.zoodb.jdo.internal.server.StorageInMemory;
import org.zoodb.jdo.internal.server.index.FreeSpaceManager;
import org.zoodb.jdo.internal.server.index.PagedOidIndex;
import org.zoodb.jdo.internal.server.index.PagedOidIndex.FilePos;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;
import org.zoodb.jdo.internal.util.CloseableIterator;

public class TestOidIndex {

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
    	FreeSpaceManager fsm = new FreeSpaceManager();
    	StorageChannel paf = new StorageInMemory(ZooConfig.getFilePageSize(), fsm);
    	//fsm.initBackingIndexLoad(paf, 7, 8);
    	fsm.initBackingIndexNew(paf);
    	//avoid returning pageId=0 for index pages in this test harness
    	fsm.getNextPage(0);
    	return paf;
    }
    
    @Test
    public void testAddStrongCheck() {
        final int MAX = 5000;
        StorageChannel paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32, 32+i);
            //			System.out.println("Inserting: " + i);
            //Now check every entry!!!
            for (int j = 1000; j <= i; j++) {
                FilePos fp2 = ind.findOid(j);
                if (fp2==null) {
                    ind.print();
                    throw new RuntimeException("j=" + j + "   i=" + i);
                }
            }
        }
        System.out.println("Index size: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());

        assertNull( ind.findOid(-1) );
        assertNull( ind.findOid(0) );
        assertNull( ind.findOid(999) );
        assertNull( ind.findOid(1000 + MAX) );
    }

    @Test
    public void testAdd() {
        final int MAX = 1000000;
        StorageChannel paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32, 32+i);
        }
        System.out.println("Index size: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());

        for (int i = 1000; i < 1000+MAX; i++) {
            FilePos fp = ind.findOid(i);
            //			System.out.println(" Looking up: " + i);
            assertEquals( 32, fp.getPage() );
            assertEquals( 32+i, fp.getOffs() );
        }

        assertNull( ind.findOid(-1) );
        assertNull( ind.findOid(0) );
        assertNull( ind.findOid(999) );
        assertNull( ind.findOid(1000 + MAX) );

        System.out.println("inner: "+ ind.statsGetInnerN() + " outer: " + ind.statsGetLeavesN());
        double epp = MAX / ind.statsGetLeavesN();
        System.out.println("Entires per page: " + epp);
    }

    @Test
    public void testIterator() {
        final int MAX = 1000000;
        StorageChannel paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);

        Iterator<FilePos> iter = ind.iterator();
        assertFalse(iter.hasNext());

        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32, 32+i);
        }

        iter = ind.iterator();
        long prev = -1;
        int n = 0;
        while (iter.hasNext()) {
            long l = iter.next().getOID();
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
        StorageChannel paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32, 32+i);
        }
        Iterator<FilePos> iter = ind.descendingIterator();
        long prev = 1000+MAX;
        int n = MAX;
        while (iter.hasNext()) {
            long l = iter.next().getOID();
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
        StorageChannel paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);
        //Fill index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32, 32+i);
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
            ind.removeOid(l);
        }
        System.out.println("Index size after delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());

        for (int i = 1000; i < 1000+MAX; i++) {
            FilePos fp = ind.findOid(i);
            if (toDelete.contains((long)i)) {
                assertNull(fp);
            } else {
                //			System.out.println(" Looking up: " + i);
                assertEquals( 32, fp.getPage() );
                assertEquals( 32+i, fp.getOffs() );
            }
        }

        //test iteration and size
        Iterator<FilePos> iter = ind.iterator();
        long prev = -1;
        int n = 0;
        while (iter.hasNext()) {
            long l = iter.next().getOID();
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
        StorageChannel paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);

        //first a simple delete on empty index
        try {
            ind.removeOid(0);
        	fail();
        } catch (NoSuchElementException e) {
        	//good!
        }

        //Fill index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32, 32+i);
        }

        System.out.println("Index size before delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());
//        int nIPagesBefore = ind.statsGetInnerN();
        int nLPagesBefore = ind.statsGetLeavesN();

        //delete index
        for (int i = 1000; i < 1000+MAX; i++) {
            long prev = ind.removeOid(i);
            assertEquals((32L<<32L) + 32L + i, prev);
        }

        System.out.println("Index size after delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());
        for (int i = 1000; i < 1000+MAX; i++) {
            FilePos fp = ind.findOid(i);
            assertNull(fp);
        }

        //test iteration and size
        Iterator<FilePos> iter = ind.iterator();
        long prev = -1;
        int n = 0;
        while (iter.hasNext()) {
            long l = iter.next().getOID();
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
            ind.insertLong(i, 32, 32+i);
            //		System.out.println("Inserting: " + i);
            //Now check every entry!!!
            for (int j = 1000; j <= i; j++) {
                FilePos fp2 = ind.findOid(j);
                if (fp2 == null) {
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
        PagedOidIndex ind = new PagedOidIndex(paf);
        //Fill index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32, 32+i);
        }

        //		int nW0 = paf.statsGetWriteCount();
        ind.write();
        int nW1 = paf.statsGetWriteCount();
        ind.insertLong(MAX * 2, 32, 32);
        ind.write();
        int nW2 = paf.statsGetWriteCount();
        assertTrue("nW1="+nW1 + " / nW2="+nW2, nW2-nW1 <= MAX_DEPTH);


        ind.removeOid(MAX * 2);
        ind.write();
        int nW3 = paf.statsGetWriteCount();
        assertTrue("nW2="+nW2 + " / nW3="+nW3, nW3-nW2 <= MAX_DEPTH);

        //TODO test more thoroughly?
    }

    @Test
    public void testMaxOid() {
        final int MAX = 1000000;
        StorageChannel paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32, 32+i);
            assertEquals(i, ind.getMaxValue());
        }

        for (int i = 1000; i < 1000+MAX; i++) {
            FilePos fp = ind.findOid(i);
            //			System.out.println(" Looking up: " + i);
            assertEquals( 32, fp.getPage() );
            assertEquals( 32+i, fp.getOffs() );
        }

        assertNull( ind.findOid(-1) );
        assertNull( ind.findOid(0) );
        assertNull( ind.findOid(999) );
        assertNull( ind.findOid(1000 + MAX) );
    }

    @Test
    public void testReverseIteratorDelete() {
        final int MAX = 1000000;
        StorageChannel paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32, 32+i);
        }

        Iterator<FilePos> iter = ind.descendingIterator();
        long prev = 1000 + MAX;
        int n = 0;
        while (iter.hasNext()) {
            long l = iter.next().getOID();
            assertTrue("l=" + l + " prev = "+ prev, l < prev );
            assertEquals( prev-1, l );
            prev = l;
            n++;
            if (l % 2 == 0) {
                ind.removeOid(l);
            }
        }
        assertEquals(MAX, n);


        //half of the should still be there
        iter = ind.descendingIterator();
        prev = 1000 + MAX + 1;
        n = 0;
        while (iter.hasNext()) {
            long l = iter.next().getOID();
            assertTrue("l=" + l + " prev = "+ prev, l < prev );
            assertEquals( prev-2, l );
            prev = l;
            n++;
            ind.removeOid(l);
       }
        assertEquals(MAX/2, n);

        //now it should be empty
        iter = ind.descendingIterator();
        assertFalse(iter.hasNext());
    }


    @Test
    public void testIteratorDelete() {
        final int MAX = 1000000;
        StorageChannel paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32, 32+i);
        }

        //Iterate while deleting every second element
        Iterator<FilePos> iter = ind.iterator();
        long prev = 1000-1;
        int n = 0;
        while (iter.hasNext()) {
            long l = iter.next().getOID();
            assertTrue("l=" + l + " prev = " + prev, l > prev );
            assertEquals( prev+1, l );
            prev = l;
            n++;
            if (l % 2 == 0) {
                ind.removeOid(l);
            }
        }
        assertEquals(MAX, n);


        //half of the should still be there
        iter = ind.iterator();
        prev = 1000-1;
        n = 0;
        while (iter.hasNext()) {
            long l = iter.next().getOID();
            assertTrue("l=" + l + " prev = " + prev, l > prev );
            assertEquals( prev+2, l );
            prev = l;
            ind.removeOid(l);
            n++;
        }
        assertEquals(MAX/2, n);

        //now it should be empty
        iter = ind.iterator();
        assertFalse(iter.hasNext());
    }

    @Test
    public void testCowIterators() {
        final int MAX = 1000000;
        StorageChannel paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);

        Iterator<FilePos> iterD = ind.descendingIterator();
        Iterator<FilePos> iterA = ind.iterator();

        //add elements
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32, 32+i);
        }

        //iterators should still be empty
        assertFalse(iterD.hasNext());
        assertFalse(iterA.hasNext());
        
        iterA = ind.iterator();
        iterD = ind.descendingIterator();
        assertTrue(iterA.hasNext());
        assertTrue(iterD.hasNext());

        //remove elements
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.removeOid(i);
        }
        
        //check newly created iterators
        Iterator<FilePos> iterAEmpty = ind.iterator();
        Iterator<FilePos> iterDEmpty = ind.descendingIterator();
        assertFalse(iterAEmpty.hasNext());
        assertFalse(iterDEmpty.hasNext());
        
        //old iterators should still have all elements available
        assertTrue(iterA.hasNext());
        assertTrue(iterD.hasNext());
        
        long prevA = 1000 - 1;
        long prevD = 1000 + MAX;
        int n = 0;
        while (iterA.hasNext() && iterD.hasNext()) {
            long l1 = iterA.next().getOID();
            long l2 = iterD.next().getOID();
            assertTrue("l=" + l1 + " prev = "+ prevA, l1 > prevA );
            assertTrue("l=" + l2 + " prev = "+ prevD, l2 < prevD );
            assertEquals( prevA+1, l1 );
            assertEquals( prevD-1, l2 );
            prevA = l1;
            prevD = l2;
            n++;
        }
        assertEquals(MAX, n);

        //iterators should now both be empty
        assertFalse(iterD.hasNext());
        assertFalse(iterA.hasNext());

        //check newly created iterators
        iterAEmpty = ind.iterator();
        iterDEmpty = ind.descendingIterator();
        assertFalse(iterAEmpty.hasNext());
        assertFalse(iterDEmpty.hasNext());
    }

    @Test
    public void testCowIterators2() {
    	//Test COW on already dirtied index.
    	//HERE: test dirtying a leaf only
        final int MAX = 1000000;
        StorageChannel paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);

        //make index a bit dirty in advance
        ind.insertLong(1000, 32, 1032);
        ind.insertLong(1001, 32, 1033);
        ind.insertLong(1002, 32, 1034);
        
        Iterator<FilePos> iterD = ind.descendingIterator();
        Iterator<FilePos> iterA = ind.iterator();

        //add other elements
        for (int i = 1010; i < 1000+MAX; i++) {
            ind.insertLong(i, 32, 32+i);
        }

        //iterators should have exactly three elements
        assertEquals(1002, iterD.next().getOID());
        assertEquals(1001, iterD.next().getOID());
        assertEquals(1000, iterD.next().getOID());
        assertFalse(iterD.hasNext());
        assertEquals(1000, iterA.next().getOID());
        assertEquals(1001, iterA.next().getOID());
        assertEquals(1002, iterA.next().getOID());
        assertFalse(iterA.hasNext());
    }
    
    @Test
    public void testCowIterators3() {
    	//Test COW on already dirtied index.
    	//Here test dirtying inner nodes as well
        final int MAX = 1000;
        final int START = 500;
        StorageChannel paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);

        //make index a bit dirty in advance
        for (int i = 1000; i < 1000 + START; i++) {
            ind.insertLong(i, 32, 32+i);
        }
        
        Iterator<FilePos> iterD = ind.descendingIterator();
        Iterator<FilePos> iterA = ind.iterator();

        //add other elements
        for (int i = 1000 + START; i < 1000+MAX; i++) {
            ind.insertLong(i, 32, 32+i);
        }

        //iterators should have exactly three elements
        for (int i = 0; i < START; i++) {
        	assertEquals(1000 + START - i - 1, iterD.next().getOID());
            assertEquals(1000 + i, iterA.next().getOID());
        }
        assertFalse(iterD.hasNext());
        assertFalse(iterA.hasNext());
        
    }
    
    @Test
    public void testCowIterators4() {
    	//Test COW on already dirtied index.
        final int MAX = 1000;
        final int START = 20;
        StorageChannel paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);

        //make index a bit dirty in advance
        for (int i = 1000; i < 1000 + START; i++) {
            ind.insertLong(i, 32, 32+i);
        }
        //Note we skip one entry here!
        for (int i = 1000 + START + 1; i < 1000 + 2*START; i++) {
            ind.insertLong(i, 32, 32+i);
        }
        
        Iterator<FilePos> iterD = ind.descendingIterator();
        Iterator<FilePos> iterA = ind.iterator();

        //add skipped element:
        int ii = 1000 + START;
        ind.insertLong(ii, 32, 32+ii);
        //add other elements
        for (int i = 1000 + START; i < 1000+MAX; i++) {
            ind.insertLong(i, 32, 32+i);
        }

        //iterators should have exactly START*2-1 elements
        for (int i = 0; i < START*2; i++) {
        	if (i == START) {
        		continue;
        	}
            assertEquals(1000+ i, iterA.next().getOID());
        }
        assertFalse(iterA.hasNext());
        for (int i = 0; i < START*2; i++) {
        	if (i == START-1) {
        		continue;
        	}
     		assertEquals(1000 + 2*START - i - 1, iterD.next().getOID());
        }
        assertFalse(iterD.hasNext());
    }

    @Test
    public void testSpaceUsage() {
        final int MAX = 1000000;
        StorageChannel paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32, 32+i);
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
        StorageChannel paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);
        for (int i = 1000; i < 2000; i++) {
            ind.insertLong(i, 32, 32+i);
        }
        for (int i = 1000+MAX-1; i >= 2000; i--) {
            ind.insertLong(i, 32, 32+i);
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
        PagedUniqueLongLong ind = new PagedUniqueLongLong(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
        }
        int root = ind.write();
//        int w0 = ind.statsGetWrittenPagesN();
//        System.out.println("w0=" + w0);

        //now read it
        PagedUniqueLongLong ind2 = new PagedUniqueLongLong(paf, root);
        int w1 = ind2.statsGetWrittenPagesN();
        Iterator<LLEntry> i = ind2.iterator(Long.MIN_VALUE, Long.MAX_VALUE);
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
        PagedUniqueLongLong ind = new PagedUniqueLongLong(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
        }
        int root = ind.write();

        //now read it
        PagedUniqueLongLong ind2 = new PagedUniqueLongLong(paf, root);
        Iterator<LLEntry> i = ind2.iterator(Long.MIN_VALUE, Long.MAX_VALUE);
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
        StorageChannel paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);
        long sum = 0;
        
        // fill index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 0, i);
            sum += i;
        }

        // overwrite with same values
        for (int i = 1000; i < 1000+MAX; i++) {
        	ind.insertLong(i, 0, i);
            sum += i;
        }

        //check element count
        Iterator<FilePos> it = ind.iterator();
        int n = 0;
        while (it.hasNext()) {
        	FilePos e = it.next();
        	assertEquals(n+1000, e.getOID());
        	assertEquals(0, e.getPage());
        	assertEquals(n+1000, e.getOffs());
        	n++;
        }
        assertEquals(MAX, n);
    	
        // overwrite with different values
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 0, i+1);
            sum += i;
        }

        //check element count
        it = ind.iterator();
        n = 0;
        while (it.hasNext()) {
        	FilePos e = it.next();
        	assertEquals(n+1000, e.getOID());
        	assertEquals(0, e.getPage());
        	assertEquals(n+1+1000, e.getOffs());
        	n++;
        }
        assertEquals(MAX, n);
    }

    
    @Test
    public void testMax() {
        StorageChannel paf = createPageAccessFile();
        PagedUniqueLongLong ind = new PagedUniqueLongLong(paf);
        
        assertEquals(Long.MIN_VALUE, ind.getMaxValue());
        
        ind.insertLong(123, 456);
        assertEquals(123, ind.getMaxValue());

        ind.insertLong(1235, 456);
        assertEquals(1235, ind.getMaxValue());

        ind.insertLong(-1235, 456);
        assertEquals(1235, ind.getMaxValue());

        ind.removeLong(123);
        assertEquals(1235, ind.getMaxValue());

        ind.removeLong(1235);
        assertEquals(-1235, ind.getMaxValue());

        ind.removeLong(-1235);
        assertEquals(Long.MIN_VALUE, ind.getMaxValue());
    }
    
    
    @Test
    public void testClear() {
        StorageChannel paf = createPageAccessFile();
        PagedUniqueLongLong ind = new PagedUniqueLongLong(paf);

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
	        assertEquals(Long.MIN_VALUE, ind.getMaxValue());
        }
        
    }

    
    //TODO test random add
    //TODO test values/pages > 63bit/31bit (MAX_VALUE?!)
    //TODO test iterator with random add


}

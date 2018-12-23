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
import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.DiskAccess;
import org.zoodb.internal.server.IOResourceProvider;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.IndexFactory;
import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.LongLongIndex.LongLongUIndex;
import org.zoodb.internal.server.index.PagedOidIndex;
import org.zoodb.internal.server.index.PagedOidIndex.FilePos;
import org.zoodb.internal.util.CloseableIterator;
import org.zoodb.tools.ZooConfig;

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

    @SuppressWarnings("unused")
	private static void println(String s) {
    	//System.out.println();
    }
    
    private IOResourceProvider createPageAccessFile() {
    	return new StorageRootInMemory(
    			ZooConfig.getFilePageSize(), DiskAccess.NULL).createChannel(DiskAccess.NULL);
    }
    
    @Test
    public void testAddStrongCheck() {
        final int MAX = 5000;
        IOResourceProvider paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32, 32+i);
            //			println("Inserting: " + i);
            //Now check every entry!!!
            for (int j = 1000; j <= i; j++) {
                FilePos fp2 = ind.findOid(j);
                if (fp2==null) {
                    ind.print();
                    throw new RuntimeException("j=" + j + "   i=" + i);
                }
            }
        }
        println("Index size: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());

        assertNull( ind.findOid(-1) );
        assertNull( ind.findOid(0) );
        assertNull( ind.findOid(999) );
        assertNull( ind.findOid(1000 + MAX) );
    }

    @Test
    public void testAdd() {
        final int MAX = 1000000;
        IOResourceProvider paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32, 32+i);
        }
        println("Index size: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());

        for (int i = 1000; i < 1000+MAX; i++) {
            FilePos fp = ind.findOid(i);
            //			println(" Looking up: " + i);
            assertEquals( 32, fp.getPage() );
            assertEquals( 32+i, fp.getOffs() );
        }

        assertNull( ind.findOid(-1) );
        assertNull( ind.findOid(0) );
        assertNull( ind.findOid(999) );
        assertNull( ind.findOid(1000 + MAX) );

        println("inner: "+ ind.statsGetInnerN() + " outer: " + ind.statsGetLeavesN());
        double epp = MAX / ind.statsGetLeavesN();
        println("Entires per page: " + epp);
    }

    @Test
    public void testIterator() {
        final int MAX = 1000000;
        IOResourceProvider paf = createPageAccessFile();
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
        IOResourceProvider paf = createPageAccessFile();
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
        IOResourceProvider paf = createPageAccessFile();
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

        println("Index size before delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());
        int nIPagesBefore = ind.statsGetInnerN();
        int nLPagesBefore = ind.statsGetLeavesN();
        for (long l: toDelete) {
            ind.removeOid(l);
        }
        println("Index size after delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());

        for (int i = 1000; i < 1000+MAX; i++) {
            FilePos fp = ind.findOid(i);
            if (toDelete.contains((long)i)) {
                assertNull(fp);
            } else {
                //			println(" Looking up: " + i);
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
        IOResourceProvider paf = createPageAccessFile();
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

        println("Index size before delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());
//        int nIPagesBefore = ind.statsGetInnerN();
        int nLPagesBefore = ind.statsGetLeavesN();

        //delete index
        for (int i = 1000; i < 1000+MAX; i++) {
            long prev = ind.removeOid(i);
            assertEquals((32L<<32L) + 32L + i, prev);
        }

        println("Index size after delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
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
            //		println("Inserting: " + i);
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
        IOResourceProvider paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);
        //Fill index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32, 32+i);
        }

        //		int nW0 = paf.statsGetWriteCount();
        paf.writeIndex(ind::write);
        int nW1 = paf.statsGetWriteCount();
        ind.insertLong(MAX * 2, 32, 32);
        paf.writeIndex(ind::write);
        int nW2 = paf.statsGetWriteCount();
        assertTrue("nW1="+nW1 + " / nW2="+nW2, nW2-nW1 <= MAX_DEPTH);


        ind.removeOid(MAX * 2);
        paf.writeIndex(ind::write);
        int nW3 = paf.statsGetWriteCount();
        assertTrue("nW2="+nW2 + " / nW3="+nW3, nW3-nW2 <= MAX_DEPTH);

        //TODO test more thoroughly?
    }

    @Test
    public void testMaxOid() {
        final int MAX = 1000000;
        IOResourceProvider paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32, 32+i);
            assertEquals(i, ind.getMaxValue());
        }

        for (int i = 1000; i < 1000+MAX; i++) {
            FilePos fp = ind.findOid(i);
            //			println(" Looking up: " + i);
            assertEquals( 32, fp.getPage() );
            assertEquals( 32+i, fp.getOffs() );
        }

        assertNull( ind.findOid(-1) );
        assertNull( ind.findOid(0) );
        assertNull( ind.findOid(999) );
        assertNull( ind.findOid(1000 + MAX) );
    }

    @Test
    public void testConcurrentModificationExceptionDescending() {
    	IOResourceProvider paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);
        for (int i = 1000; i < 2000; i++) {
            ind.insertLong(i, 32, 32+i);
        }

        //Iterate while deleting
        Iterator<FilePos> iter = ind.descendingIterator();
        long l = iter.next().getOID();
        ind.removeOid(l);
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
        long l2 = iter.next().getOID();
        ind.insertLong(l2, 22, 33);
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
        ind.insertLong(11, 22, 33);
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
    	IOResourceProvider paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);
        for (int i = 1000; i < 2000; i++) {
            ind.insertLong(i, 32, 32+i);
        }

        //Iterate while deleting
        Iterator<FilePos> iter = ind.iterator();
        long l = iter.next().getOID();
        ind.removeOid(l);
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
        long l2 = iter.next().getOID();
        ind.insertLong(l2, 22, 33);
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
        ind.insertLong(11, 22, 33);
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
        PagedOidIndex ind = new PagedOidIndex(paf);
        for (int i = 1000; i < 2000; i++) {
            ind.insertLong(i, 32, 32+i);
        }

        //Iterate while deleting
        Iterator<FilePos> iter = ind.iterator();
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
        PagedOidIndex ind = new PagedOidIndex(paf);
        for (int i = 1000; i < 2000; i++) {
            ind.insertLong(i, 32, 32+i);
        }

        //Iterate while deleting
        Iterator<FilePos> iter = ind.descendingIterator();
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

    @Test
    public void testSpaceUsage() {
        final int MAX = 1000000;
        IOResourceProvider paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32, 32+i);
        }

        
        println("inner: "+ ind.statsGetInnerN() + " outer: " + ind.statsGetLeavesN());
        double epp = MAX / ind.statsGetLeavesN();
        println("Entries per page: " + epp);
        assertTrue(epp >= PAGE_SIZE/32);
        double lpi = (ind.statsGetLeavesN() + ind.statsGetInnerN()) / ind.statsGetInnerN();
        println("Leaves per inner page: " + lpi);
        assertTrue(lpi >= PAGE_SIZE/32);
    }

    @Test
    public void testSpaceUsageReverseInsert() {
        final int MAX = 1000000;
        IOResourceProvider paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);
        for (int i = 1000; i < 2000; i++) {
            ind.insertLong(i, 32, 32+i);
        }
        for (int i = 1000+MAX-1; i >= 2000; i--) {
            ind.insertLong(i, 32, 32+i);
        }

        println("inner: "+ ind.statsGetInnerN() + " outer: " + ind.statsGetLeavesN());
        double epp = MAX / ind.statsGetLeavesN();
        println("Entries per page: " + epp + "/" + PAGE_SIZE/32);
        assertTrue(epp >= PAGE_SIZE/32);
        double lpi = (ind.statsGetLeavesN() + ind.statsGetInnerN()) / ind.statsGetInnerN();
        println("Leaves per inner page: " + lpi);
        assertTrue(lpi >= PAGE_SIZE/32);
    }


    @Test
    public void testLoadedPagesNotDirty() {
        final int MAX = 1000000;
        IOResourceProvider paf = createPageAccessFile();
        LongLongUIndex ind = IndexFactory.createUniqueIndex(PAGE_TYPE.GENERIC_INDEX, paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
        }
        int root = paf.writeIndex(ind::write);
//        int w0 = ind.statsGetWrittenPagesN();
//        println("w0=" + w0);

        //now read it
        LongLongUIndex ind2 = IndexFactory.loadUniqueIndex(PAGE_TYPE.GENERIC_INDEX, paf, root);
        int w1 = ind2.statsGetWrittenPagesN();
        Iterator<LongLongIndex.LLEntry> i = ind2.iterator(Long.MIN_VALUE, Long.MAX_VALUE);
        int n = 0;
        while (i.hasNext()) {
        	n++;
        	i.next();
        }
        paf.writeIndex(ind2::write);
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
        paf.writeIndex(ind2::write);
        int wn = ind2.statsGetWrittenPagesN();
//        println("w2=" + w2);
//        println("wn=" + wn);
        assertTrue("wn=" + wn, wn > w2);
        assertTrue("wn=" + wn, wn <= MAX_DEPTH);
        
        assertEquals(MAX, n);
    }

    
    @Test
    public void testWriting() {
        final int MAX = 1000000;
        IOResourceProvider paf = createPageAccessFile();
        LongLongUIndex ind = IndexFactory.createUniqueIndex(PAGE_TYPE.GENERIC_INDEX, paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
        }
        int root = paf.writeIndex(ind::write);

        //now read it
        LongLongUIndex ind2 = IndexFactory.loadUniqueIndex(PAGE_TYPE.GENERIC_INDEX, paf, root);
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
        IOResourceProvider paf = createPageAccessFile();
        PagedOidIndex ind = new PagedOidIndex(paf);
        
        // fill index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 0, i);
        }

        // overwrite with same values
        for (int i = 1000; i < 1000+MAX; i++) {
        	ind.insertLong(i, 0, i);
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
    	IOResourceProvider paf = createPageAccessFile();
        LongLongUIndex ind = IndexFactory.createUniqueIndex(PAGE_TYPE.GENERIC_INDEX, paf);
        
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
    	IOResourceProvider paf = createPageAccessFile();
        LongLongUIndex ind = IndexFactory.createUniqueIndex(PAGE_TYPE.GENERIC_INDEX, paf);

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
    public void testIdxWrittenPages() {
    	IOResourceProvider paf = createPageAccessFile();
        //This creates a dirty root page with no leaves
        LongLongUIndex ind = IndexFactory.createUniqueIndex(PAGE_TYPE.GENERIC_INDEX, paf);
        paf.writeIndex(ind::write);
        assertEquals(1, ind.statsGetWrittenPagesN());
        paf.writeIndex(ind::write);
        assertEquals(1+0, ind.statsGetWrittenPagesN());

        //This creates adds a leaf page and dirties the root page
        ind.insertLong(1, 2);
        paf.writeIndex(ind::write);
        assertEquals(1+2, ind.statsGetWrittenPagesN());
        paf.writeIndex(ind::write);
        assertEquals(3+0, ind.statsGetWrittenPagesN());
    }
    
    //TODO test random add
    //TODO test values/pages > 63bit/31bit (MAX_VALUE?!)
    //TODO test iterator with random add


}

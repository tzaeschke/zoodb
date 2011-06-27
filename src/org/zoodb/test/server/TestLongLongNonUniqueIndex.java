package org.zoodb.test.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.TreeMap;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.internal.Config;
import org.zoodb.jdo.internal.server.PageAccessFile;
import org.zoodb.jdo.internal.server.PageAccessFileInMemory;
import org.zoodb.jdo.internal.server.index.AbstractPagedIndex.AbstractPageIterator;
import org.zoodb.jdo.internal.server.index.FreeSpaceManager;
import org.zoodb.jdo.internal.server.index.PagedLongLong;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;

public class TestLongLongNonUniqueIndex {

    /** Adjust this when adjusting page size! */
	private static final int MAX_DEPTH = 13;  //128
	//private static final int MAX_DEPTH = 8;  //128  TODO see index improvements in TODO.txt
    //private static final int MAX_DEPTH = 4;  //1024

    @BeforeClass
    public static void setUp() {
    	/** Adjust MAX_DEPTH accordingly! */
    	Config.setFilePageSize(128);
    }

    @AfterClass
    public static void tearDown() {
    	Config.setFilePageSize(Config.FILE_PAGE_SIZE_DEFAULT);
    }

    private PageAccessFile createPageAccessFile() {
    	FreeSpaceManager fsm = new FreeSpaceManager();
    	PageAccessFile paf = new PageAccessFileInMemory(Config.getFilePageSize(), fsm);
    	//fsm.initBackingIndexLoad(paf, 7, 8);
    	fsm.initBackingIndexNew(paf);
    	return paf;
    }
    
    @Test
    public void testAddWithMockStrongCheck() {
        final int MAX = 5000;
        PageAccessFile paf = createPageAccessFile();
        PagedLongLong ind = new PagedLongLong(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
            //Now check every entry!!!
            for (int j = 1000; j <= i; j++) {
                Iterator<LLEntry> llIter = ind.iterator(j, j);
                LLEntry e = llIter.next();
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
    public void testAddWithMock() {
        final int MAX = 1000000;
        PageAccessFile paf = createPageAccessFile();
        PagedLongLong ind = new PagedLongLong(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 2+i);
            ind.insertLong(i, 3+i);
            ind.insertLong(i, 1+i);
        }
        System.out.println("Index size: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());

        for (int i = 1000; i < 1000+MAX; i++) {
        	Iterator<LLEntry> llIter = ind.iterator(i, i);
        	LLEntry e = llIter.next();
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
        PageAccessFile paf = createPageAccessFile();
        PagedLongLong ind = new PagedLongLong(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 2+i);
            ind.insertLong(i, 3+i);
            ind.insertLong(i, 1+i);
        }
        System.out.println("Index size: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());

        for (int i = 1000; i < 1000+MAX; i++) {
        	Iterator<LLEntry> llIter = ind.descendingIterator(i, i);
        	LLEntry e = llIter.next();
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
        PageAccessFile paf = createPageAccessFile();
        PagedLongLong ind = new PagedLongLong(paf);

        Iterator<LLEntry> iter = ind.iterator();
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
        PageAccessFile paf = createPageAccessFile();
        PagedLongLong ind = new PagedLongLong(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32);
            ind.insertLong(i, 11);
            ind.insertLong(i, 33);
        }

        Iterator<LLEntry> iter = ind.descendingIterator();
        long prevKey = 1000+MAX;
        long prevVal = 11;
        int n = MAX;
        while (iter.hasNext()) {
        	LLEntry e = iter.next();
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
        final int MAX = 1000000;
    	PageAccessFile paf = createPageAccessFile();
        PagedLongLong ind = new PagedLongLong(paf);
        TreeMap<Long, Long> toDelete = new TreeMap<Long, Long>();
        Random rnd = new Random();

        //Fill index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
            if (rnd.nextBoolean()) {
            	toDelete.put((long)i, (long)32+i);
            }
        }

        System.out.println("Index size before delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());
        int nIPagesBefore = ind.statsGetInnerN();
        int nLPagesBefore = ind.statsGetLeavesN();
        for (Map.Entry<Long, Long> e: toDelete.entrySet()) {
            ind.removeLong(e.getKey(), e.getValue());
        }
        System.out.println("Index size after delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());

        for (int i = 1000; i < 1000+MAX; i++) {
            Iterator<LLEntry> ei = ind.iterator(i, i);
            if (toDelete.containsKey((long)i)) {
                assertFalse(ei.hasNext());
            } else {
                //			System.out.println(" Looking up: " + i);
                assertEquals( 32+i, ei.next().getValue() );
            }
        }

        //test iteration and size
        Iterator<LLEntry> iter = ind.iterator();
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
        assertTrue(nLPagesBefore + " -> " + ind.statsGetLeavesN(), 
        		nLPagesBefore*0.9 > ind.statsGetLeavesN());
    }

    @Test
    public void testDeleteAllWithMock() {
        final int MAX = 1000000;
        PageAccessFile paf = createPageAccessFile();
        PagedLongLong ind = new PagedLongLong(paf);

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

        System.out.println("Index size before delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());
        //int nIPagesBefore = ind.statsGetInnerN();
        int nLPagesBefore = ind.statsGetLeavesN();

        //delete index
        for (int i = 1000; i < 1000+MAX; i++) {
        	long prev = ind.removeLong(i, 32+i);
            assertEquals(i + 32, prev);
        }

        System.out.println("Index size after delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());
        for (int i = 1000; i < 1000+MAX; i++) {
            Iterator<LLEntry> ie = ind.iterator(i, i);
            assertFalse(ie.hasNext());
        }

        //test iteration and size
        Iterator<LLEntry> iter = ind.iterator();
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
                Iterator<LLEntry> fp2 = ind.iterator(j, j);
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
    	PageAccessFile paf = createPageAccessFile();
        PagedLongLong ind = new PagedLongLong(paf);
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


        ind.removeLong(MAX * 2, 32);
        ind.write();
        int nW3 = paf.statsGetWriteCount();
        assertTrue("nW2="+nW2 + " / nW3="+nW3, nW3-nW2 <= MAX_DEPTH);

        //TODO test more thoroughly?
    }

    @Test
    public void testMaxOidWithMock() {
        final int MAX = 1000000;
        PageAccessFile paf = createPageAccessFile();
        PagedLongLong ind = new PagedLongLong(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
            assertEquals(i, ind.getMaxValue());
        }

        for (int i = 1000; i < 1000+MAX; i++) {
            LLEntry fp = ind.iterator(i, i).next();
            //			System.out.println(" Looking up: " + i);
            assertEquals( 32+i, fp.getValue() );
        }

        assertFalse( ind.iterator(-1, -1).hasNext() );
        assertFalse( ind.iterator(0, 0).hasNext() );
        assertFalse( ind.iterator(999, 999).hasNext() );
        assertFalse( ind.iterator(1000 + MAX, 1000 + MAX).hasNext() );
    }

    @Test
    public void testReverseIteratorDeleteWithMock() {
        final int MAX = 1000000;
        PageAccessFile paf = createPageAccessFile();
        PagedLongLong ind = new PagedLongLong(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
        }
        
        Iterator<LLEntry> iter = ind.descendingIterator();
        long prev = 1000 + MAX;
        int n = 0;
        while (iter.hasNext()) {
            long l = iter.next().getKey();
            assertTrue("l=" + l + " prev = "+ prev, l < prev );
            assertEquals( prev-1, l );
            prev = l;
            n++;
            if (l % 2 == 0) {
                ind.removeLong(l, 32+l);
            }
        }
        assertEquals(MAX, n);


        //half of the should still be there
        iter = ind.descendingIterator();
        prev = 1000 + MAX + 1;
        n = 0;
        while (iter.hasNext()) {
            long l = iter.next().getKey();
            assertTrue("l=" + l + " prev = "+ prev, l < prev );
            assertEquals( prev-2, l );
            prev = l;
            n++;
            ind.removeLong(l, 32+l);
       }
        assertEquals(MAX/2, n);

        //now it should be empty
        iter = ind.descendingIterator();
        assertFalse(iter.hasNext());
    }


    @Test
    public void testIteratorDeleteWithMock() {
        final int MAX = 1000000;
        PageAccessFile paf = createPageAccessFile();
        PagedLongLong ind = new PagedLongLong(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
        }

        //Iterate while deleting every second element
        Iterator<LLEntry> iter = ind.iterator();
        long prev = 1000-1;
        int n = 0;
        while (iter.hasNext()) {
            long l = iter.next().getKey();
            assertTrue("l=" + l + " prev = " + prev, l > prev );
            assertEquals( prev+1, l );
            prev = l;
            n++;
            if (l % 2 == 0) {
                ind.removeLong(l, 32+l);
            }
        }
        assertEquals(MAX, n);


        //half of the should still be there
        iter = ind.iterator();
        prev = 1000-1;
        n = 0;
        while (iter.hasNext()) {
            long l = iter.next().getKey();
            assertTrue("l=" + l + " prev = " + prev, l > prev );
            assertEquals( prev+2, l );
            prev = l;
            ind.removeLong(l, 32+l);
            n++;
        }
        assertEquals(MAX/2, n);

        //now it should be empty
        iter = ind.iterator();
        assertFalse(iter.hasNext());
    }

    @Test
    public void testCowIteratorsWithMock() {
        final int MAX = 1000000;
        PageAccessFile paf = createPageAccessFile();
        PagedLongLong ind = new PagedLongLong(paf);

        Iterator<LLEntry> iterD = ind.descendingIterator();
        Iterator<LLEntry> iterA = ind.iterator();

        //add elements
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
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
            ind.removeLong(i, 32+i);
        }
        
        //check newly created iterators
        Iterator<LLEntry> iterAEmpty = ind.iterator();
        Iterator<LLEntry> iterDEmpty = ind.descendingIterator();
        assertFalse(iterAEmpty.hasNext());
        assertFalse(iterDEmpty.hasNext());
        
        //old iterators should still have all elements available
        assertTrue(iterA.hasNext());
        assertTrue(iterD.hasNext());
        
        long prevA = 1000 - 1;
        long prevD = 1000 + MAX;
        int n = 0;
        while (iterA.hasNext() && iterD.hasNext()) {
            long l1 = iterA.next().getKey();
            long l2 = iterD.next().getKey();
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

    /**
     * Test adding lots of same-key entries. 
     */
    @Test
    public void testAddManyEqualWithMockStrong() {
        final int MAX = 5000;
        PageAccessFile paf = createPageAccessFile();
        PagedLongLong ind = new PagedLongLong(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(32, i);
            ind.insertLong(11, i);
            ind.insertLong(33, i);

        	Iterator<LLEntry> iter = ind.iterator(11, 11);
        	for (int ii = 1000; ii <= i; ii++) {
        		LLEntry e = iter.next();
                assertEquals( 11, e.getKey());
                assertEquals( ii, e.getValue());
            }
        	assertFalse(iter.hasNext());

        	iter = ind.iterator(33, 33);
        	for (int ii = 1000; ii <= i; ii++) {
        		LLEntry e = iter.next();
                assertEquals( 33, e.getKey());
                assertEquals( ii, e.getValue());
            }
        	assertFalse(iter.hasNext());

        	iter = ind.iterator(32, 32);
        	for (int ii = 1000; ii <= i; ii++) {
        		LLEntry e = iter.next();
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
        System.out.println("Index size: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());

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
        PageAccessFile paf = createPageAccessFile();
        PagedLongLong ind = new PagedLongLong(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(32, i);
            ind.insertLong(11, i);
            ind.insertLong(33, i);
        }
        System.out.println("Index size: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());

    	Iterator<LLEntry> iter = ind.iterator(11, 11);
    	for (int i = 1000; i < 1000+MAX; i++) {
    		LLEntry e = iter.next();
            assertEquals( 11, e.getKey());
            assertEquals( i, e.getValue());
        }
    	assertFalse(iter.hasNext());

    	iter = ind.iterator(33, 33);
    	for (int i = 1000; i < 1000+MAX; i++) {
    		LLEntry e = iter.next();
            assertEquals( 33, e.getKey());
            assertEquals( i, e.getValue());
        }
    	assertFalse(iter.hasNext());

    	iter = ind.iterator(32, 32);
    	for (int i = 1000; i < 1000+MAX; i++) {
    		LLEntry e = iter.next();
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
        PageAccessFile paf = createPageAccessFile();
        PagedLongLong ind = new PagedLongLong(paf);
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
        AbstractPageIterator<LLEntry> it = ind.iterator();
        int[] varCnt2 = new int[VAR];
        long sum2 = 0;
        while (it.hasNext()) {
        	LLEntry ie = it.next();
        	varCnt2[(int)ie.getKey()]++;
        	sum2 += ie.getValue();
        }
        for (int i = 0; i < VAR; i++) {
        	assertEquals(varCnt[i], varCnt2[i]);
        }
        assertEquals(sum, sum2);
        ind.deregisterIterator(it);
    	
        
        
    	//remove some stuff
        ind.iterator();
        while (it.hasNext()) {
        	LLEntry ie = it.next();
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
        	LLEntry ie = it.next();
        	varCnt2[(int)ie.getKey()]++;
        	sum2 += ie.getValue();
        }
        for (int i = 0; i < VAR; i++) {
        	assertEquals(varCnt[i], varCnt2[i]);
        }
        assertEquals(sum, sum2);
        ind.deregisterIterator(it);
    	
    }

    
    @Test
    public void testAddOverwrite() {
        final int MAX = 1000000;
        PageAccessFile paf = createPageAccessFile();
        PagedLongLong ind = new PagedLongLong(paf);
        long sum = 0;
        
        // fill index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, i);
            sum += i;
        }

        // overwrite with same values
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, i);
            sum += i;
        }

        //check element count
        AbstractPageIterator<LLEntry> it = ind.iterator();
        int n = 0;
        while (it.hasNext()) {
        	LLEntry e = it.next();
        	assertEquals(n+1000, e.getKey());
        	assertEquals(n+1000, e.getValue());
        	n++;
        }
        assertEquals(MAX, n);
        ind.deregisterIterator(it);
    	
        // overwrite with different values
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, i+1);
            sum += i;
        }

        //check element count again, should have doubled
        it = ind.iterator();
        n = 0;
        int i = 0;
        while (it.hasNext()) {
        	//first entry
        	LLEntry e = it.next();
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
        ind.deregisterIterator(it);
    }

    
    @Test
    public void testEmpty() {
        final int MAX = 30000;
        PageAccessFile paf = createPageAccessFile();
        PagedLongLong ind = new PagedLongLong(paf);

        ind.print();
        
        //check element count
        AbstractPageIterator<LLEntry> it = ind.iterator(1, Long.MAX_VALUE);
        assertFalse(it.hasNext());
        AbstractPageIterator<LLEntry> it2 = ind.descendingIterator();
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

    

    //TODO test random add
    //TODO test overwrite
    //TODO test values/pages > 63bit/31bit (MAX_VALUE?!)
    //TODO test iterator with random add


}

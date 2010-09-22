package org.zoodb.test.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.internal.server.PageAccessFile;
import org.zoodb.jdo.internal.server.index.PagedOidIndex;
import org.zoodb.jdo.internal.server.index.PagedOidIndex.FilePos;
import org.zoodb.test.PageAccessFileMock;
import org.zoodb.test.TestClass;
import org.zoodb.test.TestTools;

public class TestOidIndex {

    private static final String DB_NAME = "TestDb";

    @BeforeClass
    public static void setUp() {
        TestTools.createDb(DB_NAME);
        TestTools.defineSchema(DB_NAME, TestClass.class);
    }

    @AfterClass
    public static void tearDown() {
        TestTools.removeDb(DB_NAME);
    }

    @Test
    public void testAddWithMockStrongCheck() {
        final int MAX = 3000;
        PageAccessFile paf = new PageAccessFileMock();
        PagedOidIndex ind = new PagedOidIndex(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.addOid(i, 32, 32+i);
            //			System.out.println("Inserting: " + i);
            //Now check every entry!!!
            for (int j = 1000; j <= i; j++) {
                FilePos fp2 = ind.findOid(j);
                if (fp2==null) {
                    ind.print();
                    throw new RuntimeException();
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
    public void testAddWithMock() {
        final int MAX = 1000000;
        PageAccessFile paf = new PageAccessFileMock();
        PagedOidIndex ind = new PagedOidIndex(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.addOid(i, 32, 32+i);
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

    }

    @Test
    public void testIteratorWithMock() {
        final int MAX = 1000000;
        PageAccessFile paf = new PageAccessFileMock();
        PagedOidIndex ind = new PagedOidIndex(paf);

        Iterator<FilePos> iter = ind.iterator();
        assertFalse(iter.hasNext());

        for (int i = 1000; i < 1000+MAX; i++) {
            ind.addOid(i, 32, 32+i);
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
    public void testInverseIteratorWithMock() {
        final int MAX = 1000000;
        PageAccessFile paf = new PageAccessFileMock();
        PagedOidIndex ind = new PagedOidIndex(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.addOid(i, 32, 32+i);
        }

        Iterator<FilePos> iter = ind.descendingIterator();
        long prev = Long.MAX_VALUE;
        int n = MAX;
        while (iter.hasNext()) {
            long l = iter.next().getOID();
            assertTrue("l=" + l + " prev = "+ prev, l < prev );
            if (prev < Long.MAX_VALUE) {
                assertEquals( prev-1, l );
            }
            prev = l;
            n--;
        }
        assertEquals(0, n);
    }


    @Test
    public void testDeleteWithMock() {
        final int MAX = 1000000;
        PageAccessFile paf = new PageAccessFileMock();
        PagedOidIndex ind = new PagedOidIndex(paf);
        //Fill index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.addOid(i, 32, 32+i);
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
    public void testDeleteAllWithMock() {
        final int MAX = 1000000;
        PageAccessFile paf = new PageAccessFileMock();
        PagedOidIndex ind = new PagedOidIndex(paf);

        //first a simple delete on empty index
        assertFalse(ind.removeOid(0));

        //Fill index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.addOid(i, 32, 32+i);
        }

        System.out.println("Index size before delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());
        int nIPagesBefore = ind.statsGetInnerN();
        int nLPagesBefore = ind.statsGetLeavesN();

        //delete index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.removeOid(i);
        }

        System.out.println("Index size after delete: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
                ind.statsGetLeavesN());
        ind.print();
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
            System.out.println("Adding: " + i);
            ind.print();
            ind.addOid(i, 32, 32+i);
            //		System.out.println("Inserting: " + i);
            //Now check every entry!!!
            for (int j = 1000; j <= i; j++) {
                FilePos fp2 = ind.findOid(j);
                if (fp2==null) {
                    ind.print();
                    throw new RuntimeException();
                }
            }
        }
    }

    /**
     * Test that only necessary pages get dirty.
     */
    @Test
    public void testDirtyPagesWithMock() {
        final int MAX = 1000000;
        PageAccessFile paf = new PageAccessFileMock();
        PagedOidIndex ind = new PagedOidIndex(paf);
        //Fill index
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.addOid(i, 32, 32+i);
        }

        //		int nW0 = paf.statsGetWriteCount();
        ind.write();
        int nW1 = paf.statsGetWriteCount();
        ind.addOid(MAX * 2, 32, 32);
        ind.write();
        int nW2 = paf.statsGetWriteCount();
        assertTrue("nW1="+nW1 + " / nW2="+nW2, nW2-nW1 <= 4);


        ind.removeOid(MAX * 2);
        ind.write();
        int nW3 = paf.statsGetWriteCount();
        assertTrue("nW2="+nW2 + " / nW3="+nW3, nW3-nW2 <= 4);

        //TODO test more thoroughly?
    }

    @Test
    public void testMaxOidWithMock() {
        final int MAX = 1000000;
        PageAccessFile paf = new PageAccessFileMock();
        PagedOidIndex ind = new PagedOidIndex(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.addOid(i, 32, 32+i);
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
    public void testReverseIteratorDeleteWithMock() {
        final int MAX = 100;
        PageAccessFile paf = new PageAccessFileMock();
        PagedOidIndex ind = new PagedOidIndex(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.addOid(i, 32, 32+i);
        }
        ind.print(); //TODO

        Iterator<FilePos> iter = ind.descendingIterator();
        long prev = Long.MAX_VALUE;
        int n = 0;
        while (iter.hasNext()) {
            long l = iter.next().getOID();
            System.out.println("l=" + l + " prev = "+ prev);
            assertTrue("l=" + l + " prev = "+ prev, l < prev );
            if (prev < Long.MAX_VALUE) {
                assertEquals( prev-1, l );
            }
            prev = l;
            n++;
            if (l % 2 == 0) {
                ind.removeOid(l);
            }
        }
        assertEquals(MAX, n);

        ind.print(); //TODO

        //half of the should still be there
        iter = ind.descendingIterator();
        prev = Long.MAX_VALUE;
        n = 0;
        while (iter.hasNext()) {
            long l = iter.next().getOID();
            System.out.println("l=" + l + " prev = "+ prev);
            assertTrue("l=" + l + " prev = "+ prev, l < prev );
            if (prev < Long.MAX_VALUE) {
                assertEquals( prev-2, l );
            }
            prev = l;
            n++;
        }
        assertEquals(MAX/2, n);

        //now it should be empty
        iter = ind.descendingIterator();
        assertFalse(iter.hasNext());
    }


    @Test
    public void testIteratorDeleteWithMock() {
        final int MAX = 1000;
        PageAccessFile paf = new PageAccessFileMock();
        PagedOidIndex ind = new PagedOidIndex(paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.addOid(i, 32, 32+i);
        }

        //Iterate while deleting every second element
        Iterator<FilePos> iter = ind.iterator();
        long prev = Long.MIN_VALUE;
        int n = 0;
        while (iter.hasNext()) {
            long l = iter.next().getOID();
            assertTrue("l=" + l + " prev = " + prev, l > prev );
            if (prev > 0) {
                assertEquals( prev+1, l );
            }
            prev = l;
            n++;
            if (l % 2 == 0) {
                ind.removeOid(l);
            }
        }
        assertEquals(MAX, n);


        //half of the should still be there
        iter = ind.iterator();
        prev = Long.MIN_VALUE;
        n = 0;
        while (iter.hasNext()) {
            long l = iter.next().getOID();
            assertTrue("l=" + l + " prev = " + prev, l > prev );
            if (prev > 0) {
                assertEquals( prev+2, l );
            }
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
    public void testCowIteratorsWithMock() {
        final int MAX = 1000000;
        PageAccessFile paf = new PageAccessFileMock();
        PagedOidIndex ind = new PagedOidIndex(paf);

        Iterator<FilePos> iterD = ind.descendingIterator();
        Iterator<FilePos> iterA = ind.iterator();

        //add elements
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.addOid(i, 32, 32+i);
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
        
        long prev1 = Long.MAX_VALUE;
        long prev2 = Long.MIN_VALUE;
        int n = 0;
        while (iterA.hasNext() && iterD.hasNext()) {
            long l1 = iterA.next().getOID();
            long l2 = iterD.next().getOID();
            assertTrue("l=" + l1 + " prev = "+ prev1, l1 > prev1 );
            assertTrue("l=" + l2 + " prev = "+ prev2, l2 < prev2 );
            if (prev1 > Long.MIN_VALUE) {
                assertEquals( prev1+1, l1 );
            }
            if (prev2 < Long.MAX_VALUE) {
                assertEquals( prev2-1, l2 );
            }
            prev1 = l1;
            prev2 = l2;
            n++;
        }
        assertEquals(0, MAX);

        //iterators should now both be empty
        assertFalse(iterD.hasNext());
        assertFalse(iterA.hasNext());

        //check newly created iterators
        iterAEmpty = ind.iterator();
        iterDEmpty = ind.descendingIterator();
        assertFalse(iterAEmpty.hasNext());
        assertFalse(iterDEmpty.hasNext());
    }


    //TODO test random add
    //TODO test overwrite
    //TODO test values/pages > 63bit/31bit (MAX_VALUE?!)
    //TODO test iterator with random add


}

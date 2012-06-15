/*
 * Copyright 2009-2012 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.test;

import static org.junit.Assert.*;

import java.util.Collection;

import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.test.util.TestTools;

public class Test_091_IndexUpdates {

	@Before
	public void setUp() {
        TestTools.removeDb();
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
	}

    @Test
    public void testSimpleIndexUpdate() {
        TestTools.defineIndex(TestClass.class, "_long", false);
        TestTools.defineIndex(TestClass.class, "_string", true);

        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        //create data
        TestClass tc1 = new TestClass();
        tc1.setString("1");
        tc1.setLong(1);

        pm.makePersistent(tc1);
        
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        Query q = pm.newQuery(TestClass.class, "_long == 1");
        Collection<TestClass> col = (Collection<TestClass>) q.execute();
        assertEquals(1, col.size());
        q = pm.newQuery(TestClass.class, "_string == '1'");
        col = (Collection<TestClass>) q.execute();
        assertEquals(1, col.size());

        //modify
        tc1.setString("2");
        tc1.setLong(2);

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        //check old values
        q = pm.newQuery(TestClass.class, "_long == 1");
        col = (Collection<TestClass>) q.execute();
        assertTrue(col.isEmpty());
        q = pm.newQuery(TestClass.class, "_string == '1'");
        col = (Collection<TestClass>) q.execute();
        assertTrue(col.isEmpty());

        //check new values
        q = pm.newQuery(TestClass.class, "_long == 2");
        col = (Collection<TestClass>) q.execute();
        assertEquals(1, col.size());
        q = pm.newQuery(TestClass.class, "_string == '2'");
        col = (Collection<TestClass>) q.execute();
        assertEquals(1, col.size());

        //delete
        pm.deletePersistent(tc1);
        
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();
        
        //check is empty
        q = pm.newQuery(TestClass.class, "_long == 2");
        col = (Collection<TestClass>) q.execute();
        assertTrue(col.isEmpty());
        q = pm.newQuery(TestClass.class, "_string == '2'");
        col = (Collection<TestClass>) q.execute();
        assertTrue(col.isEmpty());

        
        pm.currentTransaction().commit();
        TestTools.closePM(pm);
    }

    @Test
    public void testDeletionOfModifiedObjects() {
        TestTools.defineIndex(TestClass.class, "_long", true);
        TestTools.defineIndex(TestClass.class, "_string", true);

        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        //create data
        TestClass tc1 = new TestClass();
        tc1.setString("1xyz");
        tc1.setLong(11234);

        pm.makePersistent(tc1);
        
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        //modify and delete
        tc1.setString("1-del");
        tc1.setLong(1000);
        pm.deletePersistent(tc1);

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        Query q = pm.newQuery(TestClass.class, "_long > 0");
        Collection<TestClass> col = (Collection<TestClass>) q.execute();
        assertTrue(col.isEmpty());
        
        q = pm.newQuery(TestClass.class, "_string > \"\"");
        col = (Collection<TestClass>) q.execute();
        assertTrue(col.isEmpty());

        q = pm.newQuery(TestClass.class, "_string <= \"\"");
        col = (Collection<TestClass>) q.execute();
        assertTrue(col.isEmpty());
        
        pm.currentTransaction().commit();
        TestTools.closePM(pm);
    }

    /**
     * Test what happens if a unique index is update because objects swap value
     * or one is even deleted. 
     * This should confirm that index additions occur AFTER index removals.
     */
    @Test
    public void testUniqueIndexCollision() {
        TestTools.defineIndex(TestClass.class, "_long", true);
        TestTools.defineIndex(TestClass.class, "_string", true);

        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        //create data
        TestClass tc1 = new TestClass();
        tc1.setString("1");
        TestClass tc2 = new TestClass(); 
        tc2.setString("1");

        pm.makePersistent(tc1);
        pm.makePersistent(tc2);

        //Check whether to fail immediately or only during commit (deferred).
        //... or during make persistent????
        System.err.println("FIXME JDO 3.0: Check UniqueMetadata.getDeferred()");
        
        try {
            pm.currentTransaction().commit();
            Assert.fail();
        } catch (JDOUserException e) {
            throw new RuntimeException(e);
        }

        //rollback should work.
        pm.currentTransaction().rollback();
        TestTools.closePM(pm);
    }
    
    
    /**
     * Test what happens if a unique index is update because objects swap value
     * or one is even deleted. 
     * This should confirm that index additions occur AFTER index removals.
     */
    @Test
    public void testMixedUpdateOfUniqueIndex() {
        TestTools.defineIndex(TestClass.class, "_long", true);
        TestTools.defineIndex(TestClass.class, "_string", true);

        checkMixedUpdate();
    }
    
    @Test
    public void testMixedUpdateOfNonUniqueIndex() {
        TestTools.defineIndex(TestClass.class, "_long", false);
        TestTools.defineIndex(TestClass.class, "_string", false);

        checkMixedUpdate();
    }
    
    private void checkMixedUpdate() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        final int N = 10;
        
        //create data
        TestClass[] tca1 = new TestClass[N]; 
        TestClass[] tca2 = new TestClass[N]; 
        TestClass[] tca3 = new TestClass[N]; 
        for (int i = 0; i < N; i++) {
            TestClass tc1 = new TestClass();
            tc1.setString("1-" + i);
            tc1.setLong(i);
            tc1.setInt(1);
            tca1[i] = tc1;
            
            TestClass tc2 = new TestClass();
            tc2.setString("2-" + i);
            tc2.setLong(N+i);
            tc2.setInt(2);
            tca2[N-i-1] = tc2;

            TestClass tc3 = new TestClass();
            tc3.setString("3-" + i);
            tc3.setLong(2*N+i);
            tc3.setInt(1);
            tca3[N-i-1] = tc3;
            
            pm.makePersistent(tc1);
            pm.makePersistent(tc2);
            pm.makePersistent(tc3);
        }
        
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        //modify:
        //-add new class with values of tca1
        //- change tca1 to tca2 values
        //- change tca2 to tca3 values
        //- delete tca3 values
        for (int i = 0; i < N; i++) {
            TestClass tc = new TestClass();
            tc.setString("1-" + i);
            tc.setLong(i);
            tc.setInt(4);
            pm.makePersistent(tc);

            tca1[i].setString("2-" + i);
            tca1[i].setLong(N+i);
            tca2[N-i-1].setString("3-" + i);
            tca2[N-i-1].setLong(2*N+i);
            
            pm.deletePersistent(tca3[i]);
        }

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        //check object count
        int[] res = new int[5];
        Query q = pm.newQuery(TestClass.class);
        Collection<TestClass> col = (Collection<TestClass>) q.execute();
        for (TestClass tc: col) {
            res[tc.getInt()]++;
        }
        Assert.assertEquals(0, res[0]);
        Assert.assertEquals(1, res[10]);
        Assert.assertEquals(2, res[10]);
        Assert.assertEquals(3, res[0]);
        Assert.assertEquals(4, res[10]);
        
        //check field indices
        checkQueryResult(pm, 10, 1, "_string >= '2-' && _string < '3-'");
        checkQueryResult(pm, 10, 2, "_string >= '3-'");
        checkQueryResult(pm, 0, 3, "_string >= '2-' && _string < '3-' && _int==3");
        checkQueryResult(pm, 10, 4, "_string >= '1-' && _string < '2-'");
        
        checkQueryResult(pm, 10, 1, "_long >= "+N+" && _string < "+2*N);
        checkQueryResult(pm, 10, 2, "_string >= "+2*N);
        checkQueryResult(pm, 0, 3, "_string >= "+N+" && _string < "+2*N+" && _int==3");
        checkQueryResult(pm, 10, 4, "_string >= 0 && _string < "+N);
        
//        q = pm.newQuery(TestClass.class, "_string >= '2-' && _string < '3-'");
//        col = (Collection<TestClass>) q.execute();
//        for (TestClass tc: col) {
//            Assert.assertTrue(tc.getLong() >= N && tc.getLong() < 2*N);
//            Assert.assertEquals(1, tc.getInt());
//        }
//        Assert.assertEquals(10, col.size());

        pm.currentTransaction().commit();
        TestTools.closePM(pm);
    }

    private void checkQueryResult(PersistenceManager pm, int nObj, int id, String query) {
        Query q = pm.newQuery(TestClass.class, query);
        Collection<TestClass> col = (Collection<TestClass>) q.execute();
        for (TestClass tc: col) {
            Assert.assertEquals(1, tc.getInt());
        }
        Assert.assertEquals(10, col.size());
    }
    
	@After
	public void afterTest() {
		TestTools.closePM();
		TestTools.removeDb();
	}
}

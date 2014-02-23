/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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

import javax.jdo.Extent;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.test.testutil.TestTools;

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
        TestTools.defineIndex(TestClass.class, "_float", false);
        TestTools.defineIndex(TestClass.class, "_double", true);

        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        //create data
        TestClass tc1 = new TestClass();
        tc1.setString("1");
        tc1.setLong(1);
        tc1.setFloat(1.1f);
        tc1.setDouble(1.2);

        pm.makePersistent(tc1);
        
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        checkQuery(pm, "_long == 1", 1);
        checkQuery(pm, "_string == '1'", 1);
        checkQuery(pm, "_float == 1.1", 1);
        checkQuery(pm, "_double == 1.2", 1);

        //modify
        tc1.setString("2");
        tc1.setLong(2);
        tc1.setFloat(2.1f);
        tc1.setDouble(2.2);

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        //check old values
        checkQuery(pm, "_long == 1", 0);
        checkQuery(pm, "_string == '1'", 0);
        checkQuery(pm, "_float == 1.1", 0);
        checkQuery(pm, "_double == 1.2", 0);

        //check new values
        checkQuery(pm, "_long == 2", 1);
        checkQuery(pm, "_string == '2'", 1);
        checkQuery(pm, "_float == 2.1", 1);
        checkQuery(pm, "_double == 2.2", 1);

        //delete
        pm.deletePersistent(tc1);
        
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();
        
        //check is empty
        checkQuery(pm, "_long == 2", 0);
        checkQuery(pm, "_string == '2'", 0);
        checkQuery(pm, "_float == 2.1", 0);
        checkQuery(pm, "_double == 2.2", 0);

        
        pm.currentTransaction().commit();
        TestTools.closePM(pm);
    }

    @SuppressWarnings("unchecked")
	private void checkQuery(PersistenceManager pm, String qStr, int n) {
        Query q = pm.newQuery(TestClass.class, qStr);
        Collection<TestClass> col = (Collection<TestClass>) q.execute();
        assertEquals(n, col.size());
    }
    
    @SuppressWarnings("unchecked")
	@Test
    public void testDeletionOfModifiedObjects() {
        TestTools.defineIndex(TestClass.class, "_long", true);
        TestTools.defineIndex(TestClass.class, "_string", true);
        TestTools.defineIndex(TestClass.class, "_float", true);
        TestTools.defineIndex(TestClass.class, "_double", true);

        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        //create data
        TestClass tc1 = new TestClass();
        tc1.setString("1xyz");
        tc1.setLong(11234);
        tc1.setFloat(1.1f);
        tc1.setDouble(1.2);

        pm.makePersistent(tc1);
        
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        //modify and delete
        tc1.setString("1-del");
        tc1.setLong(1000);
        tc1.setFloat(3.1f);
        tc1.setDouble(3.2);
        pm.deletePersistent(tc1);

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        checkQuery(pm, "_long > 0", 0);
        checkQuery(pm, "_string > \"\"", 0);
        checkQuery(pm, "_string <= \"\"", 0);
        
        Query q = pm.newQuery(TestClass.class);
        Collection<TestClass> col = (Collection<TestClass>) q.execute();
        assertFalse(col.iterator().hasNext());
        
        Extent<TestClass> ex = pm.getExtent(TestClass.class, false);
        assertFalse(ex.iterator().hasNext());
        
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
        System.err.println("FIXME Implement proper tests for node-revert on failed commit().");
        
        try {
            pm.currentTransaction().commit();
            Assert.fail();
        } catch (JDOUserException e) {
            //good
        }

        //rollback should work.
        pm.currentTransaction().rollback();
        TestTools.closePM(pm);
    }
    
    
    /**
     * Test what happens if a unique index is update because objects swap values.
     * The evil part is that re-ordering the objects to solve the collision is not possibles,
     * because either object needs the other one to be removed from the unique index first.
     * 
     * This is only possible with deferred update, of course.
     */
    @Test
    public void testUniqueIndexCollisionSwap() {
        TestTools.defineIndex(TestClass.class, "_long", true);
        TestTools.defineIndex(TestClass.class, "_string", true);

        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        //create data
        TestClass tc1 = new TestClass();
        tc1.setString("1");
        tc1.setInt(2);
        TestClass tc2 = new TestClass(); 
        tc2.setString("2");
        tc2.setLong(1);

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        tc1.setString("2");
        tc1.setInt(1);
        tc2.setString("1");
        tc2.setLong(2);

        //check that commit is possible
        pm.currentTransaction().commit();
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

        checkMixedUpdate(true);
    }
    
    @Test
    public void testMixedUpdateOfNonUniqueIndex() {
        TestTools.defineIndex(TestClass.class, "_long", false);
        TestTools.defineIndex(TestClass.class, "_string", false);

        checkMixedUpdate(false);
    }
    
    private void checkMixedUpdate(boolean isUnique) {
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
            tca3[i] = tc3;
            
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
        }

        if (isUnique) {
        	//if unique, we attempt a commit, which should fail. We then fix the problem and should
        	//be able to do it again.
	        try {
	        	pm.currentTransaction().commit();
	        	fail();
	        } catch (JDOUserException e) {
	        	//indeed ...
	        }
	        System.err.println("FIXME Implement proper tests for node-revert on failed commit().");
        }
        //fix the problem and try again
        for (int i = 0; i < N; i++) {
            pm.deletePersistent(tca3[i]);
        }

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        //check object count
        int[] res = new int[5];
        Query q = pm.newQuery(TestClass.class);
        @SuppressWarnings("unchecked")
		Collection<TestClass> col = (Collection<TestClass>) q.execute();
        for (TestClass tc: col) {
            res[tc.getInt()]++;
        }
        Assert.assertEquals(0, res[0]);
        Assert.assertEquals(N, res[1]);
        Assert.assertEquals(N, res[2]);
        Assert.assertEquals(0, res[3]);
        Assert.assertEquals(N, res[4]);
        
        //check field indices
        checkQueryResult(pm, N, 1, "_string >= '2-' && _string < '3-'");
        checkQueryResult(pm, N, 2, "_string >= '3-'");
        checkQueryResult(pm, 0, 3, "_string >= '2-' && _string < '3-' && _int == 3");
        checkQueryResult(pm, N, 4, "_string >= '1-' && _string < '2-'");
        
        checkQueryResult(pm, N, 1, "_long >= "+N+" && _long < "+2*N);
        checkQueryResult(pm, N, 2, "_long >= "+2*N);
        checkQueryResult(pm, 0, 3, "_long >= "+N+" && _long < "+2*N+" && _int == 3");
        checkQueryResult(pm, N, 4, "_long >= 0 && _long < "+N);
        
        pm.currentTransaction().commit();
        TestTools.closePM(pm);
    }

    private void checkQueryResult(PersistenceManager pm, int nObj, int id, String query) {
        Query q = pm.newQuery(TestClass.class, query);
        @SuppressWarnings("unchecked")
		Collection<TestClass> col = (Collection<TestClass>) q.execute();
        for (TestClass tc: col) {
            Assert.assertEquals(id, tc.getInt());
        }
        Assert.assertEquals(nObj, col.size());
    }
    
	@After
	public void afterTest() {
		TestTools.closePM();
		TestTools.removeDb();
	}
}

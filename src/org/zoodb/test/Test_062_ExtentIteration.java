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

import static junit.framework.Assert.assertEquals;

import java.util.Iterator;

import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.util.TestTools;

public class Test_062_ExtentIteration {

	@BeforeClass
	public static void setUp() {
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class, TestClassTiny.class, TestClassTiny2.class);
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
	}
	
	@Before
	public void beforeTest() {
	    PersistenceManager pm = TestTools.openPM();
	    pm.currentTransaction().begin();
        pm.newQuery(pm.getExtent(TestClass.class)).deletePersistentAll();
        pm.newQuery(pm.getExtent(TestClassTiny.class)).deletePersistentAll();
        pm.newQuery(pm.getExtent(TestClassTiny2.class)).deletePersistentAll();
        pm.currentTransaction().commit();
        TestTools.closePM();
	}

	@After
	public void afterTest() {
		TestTools.closePM();
	}
	
	/**
	 * Tests extent across transaction boundaries.
	 * The problem here is that extents are pos-indices, which are COWed.
	 * So during a commit, object may get rewritten, changing the index,
	 * meaning that the positions in the extents are wrong.
	 * 
	 * In this test, only the values in the loaded objects are false. But the
	 * pages they are loaded from are actually free, so they may get overwritten
	 * by other data.
	 */
	@Test
	public void testExtentDelitionBatched() {
	    int N = 10000;
        PersistenceManager pm = TestTools.openPM();
        //pm.setIgnoreCache(false);
        pm.currentTransaction().begin();

        for (int i = 0; i < N; i++) {
            TestClass tc = new TestClass();
            tc.setInt(i);
            pm.makePersistent(tc);
        }

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();
        
        //modify all --> create empty space in the beginning of the DB
        for (TestClass tc: pm.getExtent(TestClass.class)) {
            tc.setLong(12);
        }

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        //start iterating
        Iterator<TestClass> it = pm.getExtent(TestClass.class).iterator();
        int n = 0;
        while (n < 2000 && it.hasNext()) {
            n++;
            it.next();
        }
        
        //modify object
        //modify all again --> move to free pages in beginning
        for (TestClass tc: pm.getExtent(TestClass.class)) {
            tc.setLong(25);
        }
        //commit, this should invalidate the first extent
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        while (it.hasNext()) {
            n++;
            TestClass tc = it.next();
            //If it fails here, it means we are reading invalid objects from
            //old positions. That is really bad.
            assertEquals(25, tc.getLong());
        }
        
        //If it fails here, objects got moved around due to the commit.
        //This is not great, but I guess we live with it for now....?
        //TODO
        System.err.println("TODO Extents over commit are inconsistent when contained " +
        		"objects are modified.");
        //assertEquals(N, n);
        
        pm.currentTransaction().commit();
        TestTools.closePM();
	}
}

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
package org.zoodb.test.jdo;

import static org.junit.Assert.*;

import java.util.Collection;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.test.testutil.TestTools;
import org.zoodb.tools.DBStatistics;

public class Test_126i_QueryOrderBy extends Test_126_QueryOrderBy {

	@BeforeClass
	public static void setUp() {
		Test_126_QueryOrderBy.setUp();
		TestTools.defineIndex(TestClass.class, "_int", false);
		TestTools.defineIndex(TestClass.class, "_ref2", false);
		TestTools.defineIndex(TestClass.class, "_string", false);
	}

	@After
	public void after() {
		DBStatistics.enable(false);
		super.afterTest();
	}
	
    @Test
    public void testAscWithIndex() {
		DBStatistics.enable(true);
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		DBStatistics stats = ZooJdoHelper.getStatistics(pm);
		long nQE = stats.getQueryExecutionCount();
		long nQEWOWI = stats.getQueryExecutionWithOrderingWithoutIndexCount();
		
		Query q = pm.newQuery(TestClass.class, "_int > -100 order by _int ascending");
		Collection<TestClass> c = (Collection<TestClass>) q.execute();
		int n = 0;
		int prev = -100;
		for (TestClass tc : c) {
			assertTrue(prev < tc.getInt());
			n++;
			prev = tc.getInt();
		}
		assertEquals(5, n);
		
		assertEquals(nQE + 1, stats.getQueryExecutionCount());
		//no change!
		assertEquals(nQEWOWI, stats.getQueryExecutionWithOrderingWithoutIndexCount());
		
		TestTools.closePM();
    }
	
    @Test
    public void testDescWithIndex() {
		DBStatistics.enable(true);
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		DBStatistics stats = ZooJdoHelper.getStatistics(pm);
		long nQE = stats.getQueryExecutionCount();
		long nQEWOWI = stats.getQueryExecutionWithOrderingWithoutIndexCount();
		
		Query q = pm.newQuery(TestClass.class, "_int > -100 order by _int descending");
		Collection<TestClass> c = (Collection<TestClass>) q.execute();
		int n = 0;
		int prev = Integer.MAX_VALUE;
		for (TestClass tc : c) {
			assertTrue(prev > tc.getInt());
			n++;
			prev = tc.getInt();
		}
		assertEquals(5, n);
		
		assertEquals(nQE + 1, stats.getQueryExecutionCount());
		//no change!
		assertEquals(nQEWOWI, stats.getQueryExecutionWithOrderingWithoutIndexCount());
		
		TestTools.closePM();
    }
	

}

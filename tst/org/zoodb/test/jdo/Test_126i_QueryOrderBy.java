/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zoodb.test.jdo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

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
	
    @SuppressWarnings("unchecked")
	@Test
    public void testAscWithIndex() {
		DBStatistics.enable(true);
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		DBStatistics stats = ZooJdoHelper.getStatistics(pm);
		long nQE = stats.getQueryExecutionCount();
		long nQEWOWI = stats.getQueryExecutionWithOrderingWithoutIndexCount();
		
		Query q = pm.newQuery(TestClass.class, "_int > -100 order by _int ascending");
		List<TestClass> c = (List<TestClass>) q.execute();
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
	
    @SuppressWarnings("unchecked")
	@Test
    public void testDescWithIndex() {
		DBStatistics.enable(true);
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		DBStatistics stats = ZooJdoHelper.getStatistics(pm);
		long nQE = stats.getQueryExecutionCount();
		long nQEWOWI = stats.getQueryExecutionWithOrderingWithoutIndexCount();
		
		Query q = pm.newQuery(TestClass.class, "_int > -100 order by _int descending");
		List<TestClass> c = (List<TestClass>) q.execute();
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

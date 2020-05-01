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

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.test.testutil.TestTools;
import org.zoodb.tools.DBStatistics;

public class Test_172_QueryStatistics {

	@Before
	public void before() {
		TestTools.removeDb();
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
		TestTools.defineSchema(TestClassTiny.class);
		DBStatistics.enable(true);
	}

	@After
	public void after() {
		TestTools.closePM();
		DBStatistics.enable(false);
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
		DBStatistics.enable(false);
	}

	
	@Test
	public void testQueryStatsWithoutData() {
		runQueries(false);
	}
	
	@Test
	public void testQueryStatsWithData() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		TestClass tc = new TestClass(); 
		pm.makePersistent(tc);
		pm.currentTransaction().commit();
		TestTools.closePM();

		runQueries(false);
	}
	
	@Test
	public void testQueryStatsWithoutDataIndexed() {
		TestTools.defineIndex(TestClass.class, "_int", false);
		
		runQueries(true);
	}
	
	@Test
	public void testQueryStatsWithDataIndexed() {
		TestTools.defineIndex(TestClass.class, "_int", false);
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		TestClass tc = new TestClass(); 
		pm.makePersistent(tc);
		pm.currentTransaction().commit();
		TestTools.closePM();

		runQueries(true);
	}
	
	private void runQueries(boolean indexed) {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		DBStatistics stats = ZooJdoHelper.getStatistics(pm);
		long nQC = stats.getQueryCompileCount();
		long nQE = stats.getQueryExecutionCount();
		long nQEWI = stats.getQueryExecutionWithoutIndexCount();
		long nQEWOWI = stats.getQueryExecutionWithOrderingWithoutIndexCount();
		
		Query q = pm.newQuery(TestClass.class);
		q.execute();
		//Empty queries are not compiled...
		nQE++;
		nQEWI++;
		checkStats(nQC, nQE, nQEWI, nQEWOWI, stats); 
		
		q.execute();
		nQE++;
		nQEWI++;
		checkStats(nQC, nQE, nQEWI, nQEWOWI, stats); 
		
		q.setFilter(" _int == 0");
		q.execute();
		nQC++;
		nQE++;
		if (!indexed) {
			nQEWI++;
		}
		checkStats(nQC, nQE, nQEWI, nQEWOWI, stats); 
		
		q.execute();
		//not recompiled...
		nQE++;
		if (!indexed) {
			nQEWI++;
		}
		checkStats(nQC, nQE, nQEWI, nQEWOWI, stats); 

		q.setOrdering("_int asc");
		q.execute();
		nQC++;
		nQE++;
		if (!indexed) {
			nQEWI++;
			nQEWOWI++;
		}
		checkStats(nQC, nQE, nQEWI, nQEWOWI, stats); 
		
		q.execute();
		//not recompiled...
		nQE++;
		if (!indexed) {
			nQEWI++;
			nQEWOWI++;
		}
		checkStats(nQC, nQE, nQEWI, nQEWOWI, stats); 
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}

	private void checkStats(long nQC, long nQE, long nQEWI, long nQEWOWI, DBStatistics stats) {
		assertEquals(nQC, stats.getQueryCompileCount());
		assertEquals(nQE, stats.getQueryExecutionCount());
		assertEquals(nQEWI, stats.getQueryExecutionWithoutIndexCount());
		assertEquals(nQEWOWI, stats.getQueryExecutionWithOrderingWithoutIndexCount());
	}	

}

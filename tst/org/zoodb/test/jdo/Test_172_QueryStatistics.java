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

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
package org.zoodb.jdo.internal.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Collection;
import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooClassProxy;
import org.zoodb.internal.query.QueryAdvice;
import org.zoodb.internal.query.QueryParser;
import org.zoodb.internal.query.QueryParserV2;
import org.zoodb.internal.query.QueryTree;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.schema.ZooClass;
import org.zoodb.test.jdo.TestClass;
import org.zoodb.test.testutil.TestTools;

/**
 * White-box test for query optimizer.
 * 
 * @author Tilmann Zaeschke
 */
public class TestQueryParser {

	private PersistenceManager pm;
	
	@Before
	public void before() {
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
		pm = TestTools.openPM(); 
		pm.currentTransaction().begin();
	}
	
	@After
	public void after() {
		pm.currentTransaction().rollback();
		TestTools.closePM();
		pm = null;
	}
	
	private ZooClassDef getDef(Class<?> cls) {
		ZooClass clsZ = ZooJdoHelper.schema(pm).getClass(cls);
		ZooClassDef def = ((ZooClassProxy)clsZ).getSchemaDef();
		return def;
	}
	
	private void checkResults(String queryFilter, int nRes) {
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		Query q = pm.newQuery(TestClass.class, queryFilter);
		Collection<?> c = (Collection<?>) q.execute();
		int n = 0;
		for (Object o: c) {
			assertNotNull(o);
			n++;
		}
		assertEquals(nRes, n);
	}
	
	private void checkAdvices(String queryFilter, int nAdv) {
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		ZooClassDef def = getDef(TestClass.class);
		QueryParser qp = new QueryParser(queryFilter, def, null, null);
		QueryTree qtn = qp.parseQuery();
		
		//no indexing
		List<QueryAdvice> advices = qtn.executeOptimizer(def, new Object[] {});
//		for (QueryAdvice a: advices) {
//			System.out.println("adv: min/max = " + a.getMin()+"/"+a.getMax()+" cls=" + a.getIndex());//.getName());
//		}
		assertEquals(nAdv, advices.size());
	}
	
	/**
	 * Test the OR splitting. A query is split up at every OR, but only if both sub-queries
	 * use index attributes.
	 * Without index there should be only one resulting query.
	 * With index there should be two resulting queries.
	 */
	@Test
	public void testOrSplitterWithoutIndex() {
		String qf = "_int < 12345 && (_short == 32000 || _short == 11) && _int >= 123"; 

		QueryParserV2 p = new QueryParserV2(qf, getDef(TestClass.class), null, null);
		List<?> list = p.tokenize(qf);
		for (Object o: list) {
			System.out.println(o);
		}
	}
	
	@Test
	public void testRangeMerging() {
		//populate:
		TestClass t1 = new TestClass();
		t1.setInt(51);
		pm.makePersistent(t1);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		String qf = "(_int > 1 && _int < 52) || _int > 50 && _int <= 123";
		
		//no indexing
		checkAdvices(qf, 1);
		checkResults(qf, 1);
		
		//indexing
		ZooJdoHelper.schema(pm).getClass(TestClass.class).createIndex("_int", true);
		checkAdvices(qf, 1);
		checkResults(qf, 1);
	}
	
	@Test
	public void testRangeSeparation() {
		//populate:
		TestClass t1 = new TestClass();
		t1.setInt(2); //test3
		TestClass t3 = new TestClass();
		t3.setInt(40); //test3
		pm.makePersistent(t1);
		pm.makePersistent(t3);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		String qf = "(_int > 1 && _int < 12) || _int > 50 && _int <= 123";
		
		//no indexing
		checkAdvices(qf, 1);
		checkResults(qf, 1);
		
		//indexing
		ZooJdoHelper.schema(pm).getClass(TestClass.class).createIndex("_int", true);
		checkAdvices(qf, 2);
		checkResults(qf, 1);
	}

	
	@Test 
	public void testThatPrintingDoesntThrowExceptions() {
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		ZooClassDef def = getDef(TestClass.class);
		QueryParser qp = new QueryParser("(_int > 1 && _int < 52) || _int > 50", def, null, null);
		QueryTree qtn = qp.parseQuery();
		assertNotNull(qtn.print());
	}
}

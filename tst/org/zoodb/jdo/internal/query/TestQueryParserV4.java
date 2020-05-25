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
package org.zoodb.jdo.internal.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Collection;
import java.util.Collections;
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
import org.zoodb.internal.query.QueryParserV4;
import org.zoodb.internal.query.QueryTree;
import org.zoodb.internal.query.QueryExecutor.VariableInstance;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.impl.PersistenceManagerImpl;
import org.zoodb.schema.ZooClass;
import org.zoodb.test.jdo.TestClass;
import org.zoodb.test.testutil.TestTools;

/**
 * White-box test for query optimizer.
 * 
 * @author Tilmann Zaeschke
 */
public class TestQueryParserV4 {

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
		return ((ZooClassProxy)clsZ).getSchemaDef();
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
		QueryParserV4 qp = new QueryParserV4(queryFilter, def, 
				Collections.emptyList(), Collections.emptyList(), null, 0, 0,
				((PersistenceManagerImpl)pm).getSession());
		QueryTree qtn = qp.parseQuery();
		
		//no indexing
		VariableInstance[] vars = qtn.executeOptimizerV4(def, Collections.emptyList(), new Object[]{});
		List<QueryAdvice> advices = vars[0].getAdvices();
//		for (QueryAdvice a: advices) {
//			System.out.println("adv: min/max = " + a.getMin()+"/"+a.getMax()+" cls=" + a.getIndex());//.getName());
//		}
		assertEquals(nAdv, advices.size());
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
		checkAdvices(qf, 0);
		checkResults(qf, 1);
		
		//indexing
		ZooJdoHelper.schema(pm).getClass(TestClass.class).createIndex("_int", true);
		checkAdvices(qf, 2);
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
		checkAdvices(qf, 0);
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

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

import java.util.ArrayList;
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
import org.zoodb.internal.query.ParameterDeclaration;
import org.zoodb.internal.query.ParameterDeclaration.DECLARATION;
import org.zoodb.internal.query.QueryParserV3;
import org.zoodb.internal.query.QueryTerm;
import org.zoodb.internal.query.QueryTree;
import org.zoodb.internal.server.index.BitTools;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.schema.ZooClass;
import org.zoodb.test.jdo.TestClass;
import org.zoodb.test.testutil.TestTools;

/**
 * White-box test for query optimizer.
 * 
 * @author Tilmann Zaeschke
 */
public class TestQueryOptimizerPv3 {

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
	
	private void checkResults(String queryFilter, int nRes, Object ... params) {
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		Query q = pm.newQuery(TestClass.class, queryFilter);
		Collection<?> c;
		if (params.length == 0) {
			c = (Collection<?>) q.execute();
		} else {
			c = (Collection<?>) q.execute(params[0]);
		}
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
		QueryParserV3 qp = new QueryParserV3(queryFilter, def, Collections.emptyList(), null, 0, 
				Long.MAX_VALUE);
		QueryTree qtn = qp.parseQuery();
		
		//no indexing
		List<QueryAdvice> advices = qtn.executeOptimizer(def, new Object[] {});
		if (nAdv != advices.size()) {
			for (QueryAdvice a: advices) {
				System.out.println("adv: min/max = " + a.getMin() + "/" + a.getMax() + 
						" cls=" + a.getIndex());//.getName());
			}
		}
		assertEquals(nAdv, advices.size());
	}
	
	private void checkAdvices(String queryFilter, int nAdv, long min, long max, Object ... params) {
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		List<ParameterDeclaration> qpList = new ArrayList<>();
		ZooClassDef def = getDef(TestClass.class);
		QueryParserV3 qp = new QueryParserV3(queryFilter, def, qpList, null, 0, Long.MAX_VALUE);
		QueryTree qtn = qp.parseQuery();
		
		//set params
		for (int i = 0; i < params.length; i++) {
			//Great hack :-) !!!
			//TODO remove this $%%# once QueryFunctions can register for Parameter values.
			QueryTerm t = qtn.termIterator().next();
			ParameterDeclaration p = new ParameterDeclaration(null, "", DECLARATION.PARAMETERS, i);
			t.setParameter(p);
			//qpList.get(i).setValue(params[i]);
		}
		
		//no indexing
		List<QueryAdvice> advices = qtn.executeOptimizer(def, params);
		for (QueryAdvice a: advices) {
			assertEquals(min, a.getMin());
			assertEquals(max, a.getMax());
			if (nAdv != advices.size()) {
				System.out.println("adv: min/max = " + a.getMin() + "/" + a.getMax() + 
						" cls=" + a.getIndex());//.getName());
			}
		}
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
		//populate:
		TestClass t1a = new TestClass();
		t1a.setInt(200); //test1
		t1a.setShort((short) 11);
		TestClass t1b = new TestClass();
		t1b.setInt(201);//test1
		t1b.setShort((short) 32000);
		pm.makePersistent(t1a);
		pm.makePersistent(t1b);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		//Equivalent to:  
		// 123 <= _int < 12345 && _short==32000 || 123 <= _int < 12345 && _short==11
		//Ideally: Split if short is indexed. Do not split (or at least merge afterwards) 
		//if _short is not indexed. If _short and _int are both indexed, it depends on the
		//selectiveness of the _int and _short ranges.
		String qf = "_int < 12345 && (_short == 32000 || _short == 11) && _int >= 123"; 
		
		//no indexing
		checkAdvices(qf, 1);
		checkResults(qf, 2);
		
		//single indexing outside OR
		ZooJdoHelper.schema(pm).getClass(TestClass.class).createIndex("_int", true);
		checkAdvices(qf, 1);
		checkResults(qf, 2);
		
		//double indexing inside OR
		ZooJdoHelper.schema(pm).getClass(TestClass.class).createIndex("_short", true);
		checkAdvices(qf, 2);
		checkResults(qf, 2);
		
		//single indexing inside OR
		ZooJdoHelper.schema(pm).getClass(TestClass.class).removeIndex("_int");
		checkAdvices(qf, 2);
		checkResults(qf, 2);
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
		QueryParserV3 qp = new QueryParserV3(
				"(_int > 1 && _int < 52) || _int > 50", def, Collections.emptyList(), null, 0, 
				Long.MAX_VALUE);
		QueryTree qtn = qp.parseQuery();
		assertNotNull(qtn.print());
	}

	
	@Test 
	public void testStringsStartsWith() {
		//populate:
		TestClass t1 = new TestClass();
		t1.setString("Alice"); 
		TestClass t2 = new TestClass();
		t2.setString("Bob");
		TestClass t3 = new TestClass();
		t3.setString(null);
		pm.makePersistent(t1);
		pm.makePersistent(t2);
		pm.makePersistent(t3);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		String qf1 = "_string.startsWith('Alice')";
		//TODO
		System.err.println("TODO test with startsWith('Alice'.toUpperCase())");
		String qf2 = "_string.startsWith('Alice')";
		
		//no indexing
		checkAdvices(qf1, 1, 0, 0);
		checkResults(qf1, 1);
		checkAdvices(qf2, 1, 0, 0);
		checkResults(qf2, 1);
		
		//indexing
		ZooJdoHelper.schema(pm).getClass(TestClass.class).createIndex("_string", true);
		checkAdvices(qf1, 1, BitTools.toSortableLongPrefixMinHash("Alice"),
				BitTools.toSortableLongPrefixMaxHash("Alice"));
		checkResults(qf1, 1);
		checkAdvices(qf2, 1, BitTools.toSortableLongPrefixMinHash("Alice"),
				BitTools.toSortableLongPrefixMaxHash("Alice"));
		checkResults(qf2, 1);
	}

	
	@Test 
	public void testStringsMatches() {
		//populate:
		TestClass t1 = new TestClass();
		t1.setString("Alice"); 
		TestClass t2 = new TestClass();
		t2.setString("Bob");
		TestClass t3 = new TestClass();
		t3.setString(null);
		pm.makePersistent(t1);
		pm.makePersistent(t2);
		pm.makePersistent(t3);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		String qf1 = "_string.matches('Alice')";
		String qf2 = "this._string.matches('Alice')";
		//should be evaluated as startsWith()
		String qf3 = "_string.matches('A.*')";
		//should be evaluated as no-index
		String qf4 = "_string.matches('.*A.*')";
		
		//no indexing
		checkAdvices(qf1, 1, 0, 0);
		checkResults(qf1, 1);
		checkAdvices(qf2, 1, 0, 0);
		checkResults(qf2, 1);
		checkAdvices(qf3, 1, 0, 0);
		checkResults(qf3, 1);
		checkAdvices(qf4, 1, 0, 0);
		checkResults(qf4, 1);
		
		//indexing
		ZooJdoHelper.schema(pm).getClass(TestClass.class).createIndex("_string", true);
		checkAdvices(qf1, 1, BitTools.toSortableLong("Alice"), BitTools.toSortableLong("Alice"));
		checkResults(qf1, 1);
		checkAdvices(qf2, 1, BitTools.toSortableLong("Alice"), BitTools.toSortableLong("Alice"));
		checkResults(qf2, 1);
		//if the regex can be evaluated as a form of 'startsWith' then we do exactly that
		checkAdvices(qf3, 1, BitTools.toSortableLongPrefixMinHash("A"), 
				BitTools.toSortableLongPrefixMaxHash("A"));
		checkResults(qf3, 1);
		checkAdvices(qf4, 1, BitTools.toSortableLongPrefixMinHash(""), 
				BitTools.toSortableLongPrefixMaxHash(""));
		checkResults(qf4, 1);
	}

	
	@Test 
	public void testRefs() {
		//populate:
		TestClass tNULL = null;
		TestClass t1 = new TestClass();
		t1.setString("Alice"); 
		TestClass t2 = new TestClass();
		t2.setString("Bob");
		TestClass t3 = new TestClass();
		t3.setString(null);
		
		t1.setRef2(t2);
		
		pm.makePersistent(t1);
		pm.makePersistent(t2);
		pm.makePersistent(t3);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//ParameterDeclaration qp = new ParameterDeclaration(TestClass.class.getName(), "_ref2", true);
		//qp.setValue(t2);
		
		String qf1 = "_ref2 == null";
		String qf2 = "_ref2 == :pc";
		
		//no indexing
		checkAdvices(qf1, 1, 0, 0);
		checkResults(qf1, 2);
		checkAdvices(qf2, 1, 0, 0, t2);
		checkResults(qf2, 1, t2);
		
		
		
		//indexing
		ZooJdoHelper.schema(pm).getClass(TestClass.class).createIndex("_ref2", false);
		checkAdvices(qf1, 1, BitTools.toSortableLong(tNULL), BitTools.toSortableLong(tNULL));
		checkResults(qf1, 2);
		checkAdvices(qf2, 1, BitTools.toSortableLong(t2), BitTools.toSortableLong(t2), t2);
		checkResults(qf2, 1, t2);
	}
}

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
import org.zoodb.internal.query.QueryOptimizer;
import org.zoodb.internal.query.QueryParser;
import org.zoodb.internal.query.QueryTreeNode;
import org.zoodb.jdo.ZooJdoSchema;
import org.zoodb.schema.ZooClass;
import org.zoodb.test.jdo.TestClass;
import org.zoodb.test.testutil.TestTools;

/**
 * White-box test for query optimizer.
 * 
 * @author Tilmann Zaeschke
 */
public class TestQueryOptimizer {

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
		ZooClass clsZ = ZooJdoSchema.locateClass(pm, cls);
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
		QueryParser qp = new QueryParser(queryFilter, def, null);
		QueryTreeNode qtn = qp.parseQuery();
		QueryOptimizer qo = new QueryOptimizer(def);
		
		//no indexing
		List<QueryAdvice> advices = qo.determineIndexToUse(qtn);
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
		ZooJdoSchema.locateClass(pm, TestClass.class).createIndex("_int", true);
		checkAdvices(qf, 1);
		checkResults(qf, 2);
		
		//double indexing inside OR
		ZooJdoSchema.locateClass(pm, TestClass.class).createIndex("_short", true);
		checkAdvices(qf, 2);
		checkResults(qf, 2);
		
		//single indexing inside OR
		ZooJdoSchema.locateClass(pm, TestClass.class).removeIndex("_int");
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
		ZooJdoSchema.locateClass(pm, TestClass.class).createIndex("_int", true);
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
		ZooJdoSchema.locateClass(pm, TestClass.class).createIndex("_int", true);
		checkAdvices(qf, 2);
		checkResults(qf, 1);
	}

	
	@Test 
	public void testThatPrintingDoesntThrowExceptions() {
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		ZooClassDef def = getDef(TestClass.class);
		QueryParser qp = new QueryParser("(_int > 1 && _int < 52) || _int > 50", def, null);
		QueryTreeNode qtn = qp.parseQuery();
		assertNotNull(qtn.print());
	}
}

package org.zoodb.jdo.internal.query;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;
import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.test.TestClass;

/**
 * White-box test for query optimizer.
 * 
 * @author Tilmann Zäschke
 */
public class TestQueryOptimizer {

	/**
	 * Test the OR splitting. A query is split up at every OR, but only if both sub-queries
	 * use index attributes.
	 * Without index there should be only one resulting query.
	 * With index there should be two resulting queries.
	 * TODO Hmm, is that really how is it supposed to be?
	 *   --> Looking below, what is the meaning of inner-outer-OR? 
	 */
	@Test
	public void testOrSplitterWithoutIndex() {
		ZooClassDef supDef = 
			ZooClassDef.createFromJavaType(ZooPCImpl.class, 1, null, null, null);
		ZooClassDef def = ZooClassDef.createFromJavaType(TestClass.class, 2, supDef, null, null);
		//Equivalent to:  
		// 123 <= _int < 12345 && _short==32000 || 123 <= _int < 12345 && _short==11
		//Ideally: Split if short is indexed. Do not split (or at least merge afterwards) 
		//if _short is not indexed. If _short and _int are both indexed, it depends on the
		//selectiveness of the _int and _short ranges.
		QueryParser qp = new QueryParser(
				"_int < 12345 && (_short == 32000 || _short == 11) && _int >= 123", def);
		QueryTreeNode qtn = qp.parseQuery();
		QueryOptimizer qo = new QueryOptimizer(def);
		
		
		//no indexing
		List<QueryAdvice> advices = qo.determineIndexToUse(qtn);
		assertEquals(1, advices.size());
		
		//single indexing outside OR
		def.getAllFieldsAsMap().get("_int").setIndexed(true);
		advices = qo.determineIndexToUse(qtn);
		assertEquals(1, advices.size());
		
		//double indexing inside OR
		def.getAllFieldsAsMap().get("_short").setIndexed(true);
		advices = qo.determineIndexToUse(qtn);
		assertEquals(2, advices.size());
		
		//single indexing inside OR
		def.getAllFieldsAsMap().get("_int").setIndexed(false);
		advices = qo.determineIndexToUse(qtn);
		assertEquals(2, advices.size());  
	}
	
	@Test
	public void testRangeMerging() {
		ZooClassDef supDef = 
			ZooClassDef.createFromJavaType(ZooPCImpl.class, 1, null, null, null);
		ZooClassDef def = ZooClassDef.createFromJavaType(TestClass.class, 2, supDef, null, null);
		QueryParser qp = new QueryParser(
				"(_int > 1 && _int < 52) || _int > 50 && _int <= 123", def);
		QueryTreeNode qtn = qp.parseQuery();
		QueryOptimizer qo = new QueryOptimizer(def);
		
		//no indexing
		List<QueryAdvice> advices = qo.determineIndexToUse(qtn);
		assertEquals(1, advices.size());
		
		//indexing
		def.getAllFieldsAsMap().get("_int").setIndexed(true);
		advices = qo.determineIndexToUse(qtn);
		assertEquals(1, advices.size());
	}
	
	@Test
	public void testRangeSeparation() {
		ZooClassDef supDef = 
			ZooClassDef.createFromJavaType(ZooPCImpl.class, 1, null, null, null);
		ZooClassDef def = ZooClassDef.createFromJavaType(TestClass.class, 2, supDef, null, null);
		QueryParser qp = new QueryParser(
				"(_int > 1 && _int < 12) || _int > 50 && _int <= 123", def);
		QueryTreeNode qtn = qp.parseQuery();
		QueryOptimizer qo = new QueryOptimizer(def);
		
		//no indexing
		List<QueryAdvice> advices = qo.determineIndexToUse(qtn);
		assertEquals(1, advices.size());
		
		//indexing
		def.getAllFieldsAsMap().get("_int").setIndexed(true);
		advices = qo.determineIndexToUse(qtn);
		assertEquals(2, advices.size());
	}
	
}

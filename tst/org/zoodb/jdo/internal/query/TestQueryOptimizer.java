package org.zoodb.jdo.internal.query;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.test.TestClass;

/**
 * White-box test for query optimizer.
 * 
 * @author Tilmann Zäschke
 */
public class TestQueryOptimizer {

	@Test
	public void testOrSplitter() {
		ZooClassDef supDef = 
			ZooClassDef.createFromJavaType(PersistenceCapableImpl.class, 1, null, null, null);
		ZooClassDef def = ZooClassDef.createFromJavaType(TestClass.class, 2, supDef, null, null);
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
			ZooClassDef.createFromJavaType(PersistenceCapableImpl.class, 1, null, null, null);
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
			ZooClassDef.createFromJavaType(PersistenceCapableImpl.class, 1, null, null, null);
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

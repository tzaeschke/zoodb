package org.zoodb.test;

import javax.jdo.PersistenceManager;

import org.junit.Before;
import org.zoodb.jdo.api.Schema;


/**
 * Perform query tests using one indexed attribute.
 * 
 * @author Tilmann Zäschke
 */
public class Test_070i_Query extends Test_070_Query {

	@Before
	public void createIndex() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		Schema s = Schema.locate(pm, TestClass.class);
		if (!s.isIndexDefined("_int")) {
			System.err.println("Defining index: TestClass._int");
			s.defineIndex("_int", true);
		}
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
}

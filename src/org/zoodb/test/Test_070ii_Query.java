package org.zoodb.test;

import javax.jdo.PersistenceManager;

import org.junit.Before;
import org.zoodb.jdo.api.Schema;

/**
 * Perform query tests when indexing all attributes.
 * 
 * @author Tilmann Zäschke
 */
public class Test_070ii_Query extends Test_070_Query {

	@Before
	public void createIndex() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		Schema s = Schema.locate(pm, TestClass.class);
		if (!s.isIndexDefined("_int")) {
			System.err.println("Defining index: TestClass._int");
			s.defineIndex("_int", true);
			s.defineIndex("_long", true);
			s.defineIndex("_byte", true);
			s.defineIndex("_short", true);
			s.defineIndex("_char", true);
//			s.defineIndex("_bool", true);
//			s.defineIndex("_string", true);
//			s.defineIndex("_float", true);
//			s.defineIndex("_double", true);
		}
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
}

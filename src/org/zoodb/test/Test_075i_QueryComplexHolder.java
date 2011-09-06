package org.zoodb.test;

import javax.jdo.PersistenceManager;

import org.junit.Before;
import org.zoodb.jdo.api.Schema;
import org.zoodb.test.data.ComplexHolder2;


/**
 * Perform query tests using one indexed attribute.
 * 
 * @author Tilmann Zäschke
 */
public class Test_075i_QueryComplexHolder extends Test_075_QueryComplexHolder {

	@Before
	public void createIndex() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		Schema s = Schema.locate(pm, ComplexHolder2.class);
		if (!s.isIndexDefined("i2")) {
			System.err.println("Defining index: TestClass._int");
			s.defineIndex("i2", false);
		}
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
}

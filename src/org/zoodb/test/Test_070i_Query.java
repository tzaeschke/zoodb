package org.zoodb.test;

import javax.jdo.PersistenceManager;

import org.junit.Before;
import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.test.util.TestTools;


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
		ZooClass s = ZooSchema.locateClass(pm, TestClass.class);
		if (!s.isIndexDefined("_int")) {
			s.defineIndex("_int", false);
		}
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
}

package org.zoodb.test;

import javax.jdo.PersistenceManager;

import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.test.data.ComplexHolder2;
import org.zoodb.test.util.TestTools;


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
		ZooSchema s = ZooSchema.locate(pm, ComplexHolder2.class);
		if (!s.isIndexDefined("i2")) {
			s.defineIndex("i2", false);
		}
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
	/**
	 * This failed with DataStreamCorrupted exception if makeDirty() was not called when setting
	 * attributes. In that case, the update() failed for most objects.
	 * It is unclear how this corrupted the database.
	 * -> Temporary fix: do not set objects clean in ClientSessionCache.postCommit().
	 * 
	 * The problem was that the _usedClasses in the DataSerializer were reused for objects on the 
	 * written in the same stream (same page). Randomly accessing these objects meant that the
	 * required used classes were not available, because they were only store with the first object.
	 */
	@Test
	@Override
	public void test() {
//		run(1, 1, 4);

//		run(1, 50, 4);
//		run(2, 50, 4);
//		run(3, 50, 4);
//		run(5, 500, 6);
		run(6, 500, 6);
		run(7, 500, 6);
	}	
}

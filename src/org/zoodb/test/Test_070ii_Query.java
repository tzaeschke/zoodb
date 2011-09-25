package org.zoodb.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.Schema;
import org.zoodb.test.util.TestTools;

/**
 * Perform query tests after indexing all attributes.
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
			s.defineIndex("_int", false);
			s.defineIndex("_long", false);
			s.defineIndex("_byte", false);
			s.defineIndex("_short", false);
			s.defineIndex("_char", false);
//			s.defineIndex("_bool", true);
			s.defineIndex("_string", false);
//			s.defineIndex("_float", true);  //TODO
//			s.defineIndex("_double", true);  //TODO
		}
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
	
	/**
	 * Create two sub-queries with different indices.
	 * The result should not contain duplicates.
	 */
	@SuppressWarnings("unchecked")
    @Test
	public void testDualIndexingWithOrSplit() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        Query q = pm.newQuery("SELECT FROM " + TestClass.class.getName());
        assertEquals(pm, q.getPersistenceManager());
        assertFalse(q.isUnmodifiable());

        Collection<TestClass> r;
        
        // OR
        q.setFilter("_int < 12345 && (_short == 32000 || _string == 'xyz') && _int >= 123");
        r = (Collection<TestClass>) q.execute();
        assertEquals(2, r.size());
        for (TestClass tc: r) {
            assertTrue("int=" + tc.getInt(), tc.getInt() >= 123);
        }

        TestTools.closePM(pm);
	}
}

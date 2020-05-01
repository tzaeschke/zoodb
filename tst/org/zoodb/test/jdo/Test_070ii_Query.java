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
package org.zoodb.test.jdo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.schema.ZooClass;
import org.zoodb.test.testutil.TestTools;

/**
 * Perform query tests after indexing all attributes.
 * 
 * @author Tilmann Zaeschke
 */
public class Test_070ii_Query extends Test_070_Query {

	private long executedWithoutIndex = 0;
	
	@Before
	public void createIndex() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass s = ZooJdoHelper.schema(pm).getClass(TestClass.class);
		if (!s.hasIndex("_int")) {
			s.createIndex("_int", false);
			s.createIndex("_long", false);
			s.createIndex("_byte", false);
			s.createIndex("_short", false);
			s.createIndex("_char", false);
//			s.defineIndex("_bool", true);
			s.createIndex("_string", false);
			s.createIndex("_float", false); 
			s.createIndex("_double", false);
		}
		executedWithoutIndex = ZooJdoHelper.getStatistics(pm).getQueryExecutionWithoutIndexCount();
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
	@After
	public void checkIndexUsage() {
		super.afterTest();
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		long executedWithoutIndex2 = ZooJdoHelper.getStatistics(pm).getQueryExecutionWithoutIndexCount();
		pm.currentTransaction().commit();
		TestTools.closePM();
		assertEquals(executedWithoutIndex, executedWithoutIndex2);
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
        int n = 0;
        for (TestClass tc: r) {
            assertTrue("int=" + tc.getInt(), tc.getInt() >= 123);
            assertTrue(tc.getInt() >= 123);
            n++;
        }
        assertEquals(2, n);

        TestTools.closePM(pm);
	}
}

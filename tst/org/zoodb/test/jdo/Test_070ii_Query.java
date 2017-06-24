/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
 * 
 * This file is part of ZooDB.
 * 
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * See the README and COPYING files for further information. 
 */
package org.zoodb.test.jdo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

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

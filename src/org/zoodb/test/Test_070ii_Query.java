/*
 * Copyright 2009-2012 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.api.ZooSchema;
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
		ZooClass s = ZooSchema.locateClass(pm, TestClass.class);
		if (!s.isIndexDefined("_int")) {
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
        for (TestClass tc: r) {
            assertTrue("int=" + tc.getInt(), tc.getInt() >= 123);
            System.out.println("tt-tc=" + tc.getInt());
        }
        assertEquals(2, r.size());

        TestTools.closePM(pm);
	}
}

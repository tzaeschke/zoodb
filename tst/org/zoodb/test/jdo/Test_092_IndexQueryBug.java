/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
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

import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.testutil.TestTools;

/**
 * Tests for query setOrdering().
 * 
 * @author ztilmann
 *
 */
public class Test_092_IndexQueryBug {

	@BeforeClass
	public static void setUp() {
        TestTools.removeDb();
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
    	TestTools.defineSchema(TestQueryClass.class);
	}
	
	private void populate() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        pm.newQuery(TestClass.class).deletePersistentAll();
        
        TestClass tc1 = new TestClass();
        tc1.setData(1, false, 'c', (byte)127, (short)32001, 1234567890L, "xyz5", new byte[]{1,2},
        		-1.1f, 35, null);
        pm.makePersistent(tc1);
        
        pm.currentTransaction().commit();
        TestTools.closePM();
	}
		
	@After
	public void afterTest() {
		TestTools.closePM();
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
	}


	@Test
	public void testIssue109() {
		populate();
		populate();
	
		TestTools.defineIndex(TestClass.class, "_string", true);
		TestTools.removeIndex(TestClass.class, "_string");
		
		populate();
		TestTools.removeIndex(TestClass.class, "_string");
		//This fails with page type mismatch...
		TestTools.openPM();
		TestTools.closePM();
	}

}

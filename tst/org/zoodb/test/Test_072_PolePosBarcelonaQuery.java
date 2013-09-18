/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
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

import java.util.Collection;
import java.util.Iterator;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.data.JB0;
import org.zoodb.test.data.JB1;
import org.zoodb.test.data.JB2;
import org.zoodb.test.data.JB3;
import org.zoodb.test.data.JB4;
import org.zoodb.test.testutil.TestTools;

public class Test_072_PolePosBarcelonaQuery {

	private static final String DB_NAME = "TestDb";
	private static final int COUNT = 1000;
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb(DB_NAME);
		TestTools.defineSchema(DB_NAME, JB0.class);
		TestTools.defineSchema(DB_NAME, JB1.class);
		TestTools.defineSchema(DB_NAME, JB2.class);
		TestTools.defineSchema(DB_NAME, JB3.class);
		TestTools.defineSchema(DB_NAME, JB4.class);

		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		for (int i = 1; i <= COUNT; i++) {
			JB0 jb;
			jb = new JB4(4, 4, i, 4, 4);
			pm.makePersistent(jb);
			jb = new JB3(3, 3, i, 3);
			pm.makePersistent(jb);
			jb = new JB2(2, 2, i);
			pm.makePersistent(jb);
			jb = new JB1(1, 2);
			pm.makePersistent(jb);
			jb = new JB0(0);
			pm.makePersistent(jb);
		}
		
		pm.currentTransaction().commit();
		TestTools.closePM(pm);
	}

	@Test
	public void testBarcelonaQueryJB3() {
		System.out.println("Testing Query()");
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		//special:
		// - uses "this."
		// - uses param from superclass
		// - asks for a field that is not present in all super classes
		// - this should not return sub-class instances
		String filter = "this.b2 == param";
		for (int i = 1; i <= COUNT; i++) {
			Query query = pm.newQuery(JB3.class, filter);
			query.declareParameters("int param");
			assertEquals( 2, doQuery(query, i) );
		}
		
		TestTools.closePM(pm);
	}

	@Test
	public void testBarcelonaQueryJB4() {
		System.out.println("Testing Query()");
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		//special:
		// - uses "this."
		// - uses param from superclass
		// - asks for a field that is not present in all super classes
		String filter = "this.b2 == param";
		for (int i = 1; i <= COUNT; i++) {
			Query query = pm.newQuery(JB4.class, filter);
			query.declareParameters("int param");
			assertEquals( 1, doQuery(query, i) );
		}
		
		TestTools.closePM(pm);
	}

    private int doQuery(Query q, Object param) {
        Collection<?> result = (Collection<?>)q.execute(param);
        Iterator<?> it = result.iterator();
        int n = 0;
        while (it.hasNext()){
            it.next();
            n++;
        }
        return n;
    }
	
	@Test
	public void testBarcelonaQueryJB4Field() {
		System.out.println("Testing Query()");
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		//special:
		// - uses "this."
		// - uses param from superclass
		String filter = "this.b1 == param";
		Query query = pm.newQuery(JB4.class, filter);
		query.declareParameters("int param");
		assertEquals( COUNT, doQuery(query, 4) );
		
		TestTools.closePM(pm);
	}

	@Test
	public void testBarcelonaQueryJB4Value() {
		System.out.println("Testing Query()");
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		//special:
		// - uses "this."
		// - uses param from superclass
		String filter = "this.b4 <= param";
		Query query = pm.newQuery(JB4.class, filter);
		query.declareParameters("int param");
		assertEquals( COUNT, doQuery(query, 4) );
		
		TestTools.closePM(pm);
	}

	
	@After
	public void afterTest() {
		TestTools.closePM();
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb(DB_NAME);
	}
	
}

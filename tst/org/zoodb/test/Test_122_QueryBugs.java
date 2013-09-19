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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.testutil.TestTools;

/**
 * Tests for query setResult().
 * 
 * @author ztilmann
 *
 */
public class Test_122_QueryBugs {

	@BeforeClass
	public static void setUp() {
        TestTools.removeDb();
		TestTools.createDb();
	}

	@Before
	public void before() {
		TestTools.defineSchema(TestClass.class);
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        pm.newQuery(TestClass.class).deletePersistentAll();
        
        TestClass tc1 = new TestClass();
        tc1.setData(1, false, 'c', (byte)127, (short)32001, 1234567890L, "xyz1", new byte[]{1,2},
        		-1.1f, 35);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(12, false, 'd', (byte)126, (short)32002, 1234567890L, "xyz2", new byte[]{1,2},
        		-0.1f, 34);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(123, false, 'e', (byte)125, (short)32003, 1234567890L, null, new byte[]{1,2},
        		0.1f, 3.0);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(1234, false, 'f', (byte)124, (short)32004, 1234567890L, "xyz", new byte[]{1,2},
        		1.1f, -0.01);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(12345, false, 'g', (byte)123, (short)32005, 1234567890L, "xyz", new byte[]{1,2},
        		11.1f, -35);
        pm.makePersistent(tc1);
        
        pm.currentTransaction().commit();
        TestTools.closePM();;
	}
		
	@After
	public void afterTest() {
		TestTools.closePM();
		//also removes indexes and objects
		TestTools.removeSchema(TestClass.class);
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
	}


    /**
     * See issue #20.
     */
    @Test
    public void testStringIndex() {
    	TestTools.defineIndex(TestClass.class, "_string", false);
    	testStringQuery();
    }
	
    /**
     * See issue #20.
     */
    @Test
    public void testStringQuery() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		//System.out.println(ZooSchema.locateClass(pm, TestClass.class).hasIndex("_string"));
		
		Query q = null; 
		Collection<?> r;
		
		q = pm.newQuery(TestClass.class, "_string != 'xyz'");
		r = (Collection<?>)q.execute();
		assertEquals(3, r.size());
				
		q = pm.newQuery(TestClass.class, "_string == null");
		r = (Collection<?>)q.execute();
		assertEquals(1, r.size());
				
		q = pm.newQuery(TestClass.class, "_string != null");
		r = (Collection<?>)q.execute();
		assertEquals(4, r.size());
    }
	
    /**
     * See issue #21.
     */
    @Test
    public void testSetFilterForParameters() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		Collection<?> r;
		
		q = pm.newQuery(TestClass.class, "_int == :x");
		q.setFilter("_int == 123");
		r = (Collection<?>)q.execute();
		assertEquals(1, r.size());
    }
    
 	@Test
 	public void testQueryString_Issue26() {
 		//also removes indexes and objects
 		TestTools.removeSchema(TestClass.class);
 		TestTools.defineSchema(TestClass.class);

  		PersistenceManager pm0 = TestTools.openPM();
 		pm0.currentTransaction().begin();
 		
 		TestClass t1 = new TestClass();
 		TestClass t2 = new TestClass();
 		TestClass t3 = new TestClass();
 		t1.setString(null);
 		t2.setString("lalalala");
 		t3.setString("lala");
 		pm0.makePersistent(t1);
 		pm0.makePersistent(t2);
 		pm0.makePersistent(t3);
 		
 		long oid1 = (Long) pm0.getObjectId(t1);
 		long oid2 = (Long) pm0.getObjectId(t2);
 		long oid3 = (Long) pm0.getObjectId(t3);

 		//close session
 		pm0.currentTransaction().commit();
 		TestTools.closePM();

 		//query
 		PersistenceManager pm = TestTools.openPM();
 		pm.currentTransaction().begin();

 		Query q = pm.newQuery(TestClass.class, "_string == 'haha'");
 		Collection<?> c = (Collection<?>) q.execute();
 		assertEquals(0, c.size());

 		q = pm.newQuery(TestClass.class, "_string == 'lalalala'");
 		c = (Collection<?>) q.execute();
 		assertEquals(1, c.size());
 		Iterator<?> it = c.iterator(); 
 		assertEquals(oid2, pm.getObjectId(it.next()));

 		//These used to fail because the comparison in the query evaluator expected -1/1 as only
 		//possible outcomes of value comparison
 		q = pm.newQuery(TestClass.class, "!(_string == 'haha')");
 		c = (Collection<?>) q.execute();
 		assertEquals(3, c.size());
 		it = c.iterator(); 
 		assertEquals(oid1, pm.getObjectId(it.next()));
 		assertEquals(oid2, pm.getObjectId(it.next()));
 		assertEquals(oid3, pm.getObjectId(it.next()));

 		q = pm.newQuery(TestClass.class, "_string != 'haha'");
 		c = (Collection<?>) q.execute();
 		assertEquals(3, c.size());
 		it = c.iterator(); 
 		assertEquals(oid1, pm.getObjectId(it.next()));
 		assertEquals(oid2, pm.getObjectId(it.next()));
 		assertEquals(oid3, pm.getObjectId(it.next()));
 		TestTools.closePM();
 	}

    
 	@Test
 	public void testIndexStringWithIndex_Issue26b() {
 		//also removes indexes and objects
 		TestTools.removeSchema(TestClass.class);
 		TestTools.defineSchema(TestClass.class);

		TestTools.defineIndex(TestClass.class, "_string", true);
 		
 		PersistenceManager pm0 = TestTools.openPM();
 		pm0.currentTransaction().begin();
 		
 		TestClass t1 = new TestClass();
 		TestClass t2 = new TestClass();
 		TestClass t3 = new TestClass();
 		t1.setString(null);
 		t2.setString("lalalala");
 		t3.setString("lala");
 		pm0.makePersistent(t1);
 		pm0.makePersistent(t2);
 		pm0.makePersistent(t3);
 		
 		long oid1 = (Long) pm0.getObjectId(t1);
 		long oid2 = (Long) pm0.getObjectId(t2);
 		long oid3 = (Long) pm0.getObjectId(t3);

 		//close session
 		pm0.currentTransaction().commit();
 		TestTools.closePM();

 		//query
 		PersistenceManager pm = TestTools.openPM();
 		pm.currentTransaction().begin();

 		Query q = pm.newQuery(TestClass.class, "_string == 'haha'");
 		Collection<?> c = (Collection<?>) q.execute();
 		assertEquals(0, c.size());

 		q = pm.newQuery(TestClass.class, "_string == 'lalalala'");
 		c = (Collection<?>) q.execute();
 		assertEquals(1, c.size());
 		Iterator<?> it = c.iterator(); 
 		assertEquals(oid2, pm.getObjectId(it.next()));

 		//These used to fail because the comparison in the query evaluator expected -1/1 as only
 		//possible outcomes of value comparison
 		q = pm.newQuery(TestClass.class, "!(_string == 'haha')");
 		c = (Collection<?>) q.execute();
 		assertEquals(3, c.size());
 		it = c.iterator(); 
 		assertEquals(oid1, pm.getObjectId(it.next()));
 		assertEquals(oid3, pm.getObjectId(it.next()));
 		assertEquals(oid2, pm.getObjectId(it.next()));

 		q = pm.newQuery(TestClass.class, "_string != 'haha'");
 		c = (Collection<?>) q.execute();
 		assertEquals(3, c.size());
 		it = c.iterator(); 
 		assertEquals(oid1, pm.getObjectId(it.next()));
 		assertEquals(oid3, pm.getObjectId(it.next()));
 		assertEquals(oid2, pm.getObjectId(it.next()));
 		TestTools.closePM();
 	}

}

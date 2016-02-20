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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Iterator;

import javax.jdo.JDOUserException;
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


 	/**
 	 * When missing String delimiters ' or ", then the error message should point that out.
 	 */
  	@Test
  	public void testIndexStringWithIndex_Issue46() {
  		//also removes indexes and objects
  		TestTools.removeSchema(TestClass.class);
  		TestTools.defineSchema(TestClass.class);

  		PersistenceManager pm = TestTools.openPM();
  		pm.currentTransaction().begin();
  		
  		try {
  			Query q = pm.newQuery(TestClass.class, "_string == Bug");
  			q.execute();
  			fail();
  		} catch (JDOUserException e) {
  			String m = e.getMessage();
  			assertTrue(m, m.contains("\""));
  		}
  		try {
  			Query q = pm.newQuery(TestClass.class, "_string == Bug 46");
  			q.execute();
  			fail();
  		} catch (JDOUserException e) {
  			String m = e.getMessage();
  			assertTrue(m, m.contains("nexpected characters"));
  		}
  		TestTools.closePM();
  	}
  	
 	@Test
  	public void testQueryIterationWhileModifyingCache() {
  		//also removes indexes and objects
  		TestTools.removeSchema(TestClass.class);
  		TestTools.defineSchema(TestClass.class);
  		TestTools.defineIndex(TestClass.class, "_string", true);

  		PersistenceManager pm = TestTools.openPM();
  		pm.setIgnoreCache(false);
  		pm.currentTransaction().begin();
  		
  		TestClass t1 = new TestClass();
  		t1.setString("1");
  		pm.makePersistent(t1);
  		pm.currentTransaction().commit();
  		pm.currentTransaction().begin();
  		
  		TestClass t2 = new TestClass();
  		t2.setString("2");
  		pm.makePersistent(t2);
 		TestClass t3 = new TestClass();
  		t3.setString("3");
  		pm.makePersistent(t3);
		pm.currentTransaction().commit();
  		pm.currentTransaction().begin();

		Query qS = pm.newQuery(TestClass.class, "_string=='1'");
		@SuppressWarnings("unchecked")
		Collection<TestTools> cS = (Collection<TestTools>) qS.execute();

		Query qI = pm.newQuery(TestClass.class, "_int==0");
		@SuppressWarnings("unchecked")
		Collection<TestTools> cI = (Collection<TestTools>) qI.execute();
		
  		t1.setString("x1");
  		t2.setString("x2");
  		t3.setString("x3");
  		t1.setInt(11);
  		t1.setInt(22);
  		t1.setInt(33);

  		TestClass t4 = new TestClass();
  		t4.setString("1");
  		pm.makePersistent(t4);

  		int nS = 0;
  		Iterator<?> itS = cS.iterator();
  		while (itS.hasNext()) {
  			nS++;
  			itS.next();
  		}
  		assertEquals(1, nS);
  		
  		int nI = 0;
  		Iterator<?> itI = cI.iterator();
  		while (itI.hasNext()) {
  			nI++;
  			itI.next();
  		}
  		//assertEquals(1, nI);
  		//3 implies that the collection was created before the the iterator traversed the objects
  		assertEquals(3, nI);  
  		
   		TestTools.closePM();
  	}
 	
 	/**
 	 * The problem is that the query evaluator simply loaded ALL dirty objects from the cache into
 	 * the query processor, without checking whether they have the correct class.
 	 * 
 	 * Requires: 
 	 * - Index on queried class (TestClassTiny)
 	 * - no commit after new TestClass instance 
 	 */
	@Test
  	public void testQueryFieldAccess_Issue_53() {
 		TestTools.removeSchema(TestClass.class);
  		TestTools.defineSchema(TestClass.class);
  		//also removes indexes and objects
  		TestTools.removeSchema(TestClassTiny.class);
  		TestTools.defineSchema(TestClassTiny.class);
  		TestTools.defineIndex(TestClassTiny.class, "_int", true);

  		PersistenceManager pm = TestTools.openPM();
  		pm.setIgnoreCache(false);
  		pm.currentTransaction().begin();
  		
  		TestClassTiny tt = new TestClassTiny();
  		tt.setInt(1);
  		pm.makePersistent(tt);
  		pm.currentTransaction().commit();
  		pm.currentTransaction().begin();

 		TestClass t4 = new TestClass();
  		pm.makePersistent(t4);
  		
//  		javax.jdo.JDOFatalInternalException: Cannot access field: _int class="org.zoodb.test.jdo.TestClass", declaring class="org.zoodb.test.jdo.TestClassTiny"
//  				at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
//  				at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:57)
//  				at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
//  				at java.lang.reflect.Constructor.newInstance(Constructor.java:526)
//  				at org.zoodb.internal.util.ReflTools.newInstance(ReflTools.java:211)
//  				at org.zoodb.internal.util.DBLogger.newEx(DBLogger.java:97)
//  				at org.zoodb.internal.util.DBLogger.newEx(DBLogger.java:88)
//  				at org.zoodb.internal.util.DBLogger.newFatalInternal(DBLogger.java:187)
//  			NestedThrowablesStackTrace:
//  			java.lang.IllegalArgumentException: Cannot set int field org.zoodb.test.jdo.TestClassTiny._int to org.zoodb.test.jdo.TestClass
//  				at sun.reflect.UnsafeFieldAccessorImpl.throwSetIllegalArgumentException(UnsafeFieldAccessorImpl.java:164)
//  				at sun.reflect.UnsafeFieldAccessorImpl.throwSetIllegalArgumentException(UnsafeFieldAccessorImpl.java:168)
//  				at sun.reflect.UnsafeFieldAccessorImpl.ensureObj(UnsafeFieldAccessorImpl.java:55)
//  				at sun.reflect.UnsafeIntegerFieldAccessorImpl.getInt(UnsafeIntegerFieldAccessorImpl.java:56)
//  				at sun.reflect.UnsafeIntegerFieldAccessorImpl.get(UnsafeIntegerFieldAccessorImpl.java:36)
//  				at java.lang.reflect.Field.get(Field.java:379)
//  				at org.zoodb.internal.query.QueryTerm.evaluate(QueryTerm.java:77)
//  				at org.zoodb.internal.query.QueryTreeNode.evaluate(QueryTreeNode.java:121)
//  				at org.zoodb.jdo.impl.QueryImpl.applyQueryOnExtent(QueryImpl.java:486)
//  				at org.zoodb.jdo.impl.QueryImpl.runQuery(QueryImpl.java:526)
//  				at org.zoodb.jdo.impl.QueryImpl.execute(QueryImpl.java:600)
//  				at org.zoodb.test.jdo.Test_122_QueryBugs.testQueryFieldAccess_IssueXXX2(Test_122_QueryBugs.java:411)
  		
		Query qtt = pm.newQuery(TestClassTiny.class, "_int==1");
		@SuppressWarnings("unchecked")
		Collection<TestTools> ctt = (Collection<TestTools>) qtt.execute();
		assertEquals(1, ctt.size());

		TestTools.closePM();
  	}
 	
	@Test
	public void testParsing1() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		try {
			//this was found by the fuzzying tool
			Query q = pm.newQuery(TestClass.class, "<=<dcontains0or.'");
			q.execute();
			fail();
		} catch (JDOUserException e) {
			//good
			assertTrue(e.getMessage().contains("unexpected end"));
		}
	}
 	
	@Test
	public void testParsing2() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		try {
			//this was found by the fuzzying tool
			Query q = pm.newQuery(TestClass.class, 
					"!=.isEmpty,this,9or0contains Lcontains<= &(and.contains(");
			q.execute();
			fail();
		} catch (JDOUserException e) {
			//good
			assertTrue(e.getMessage().contains("Comparator expected near position"));
		}
	}
	
	@Test
	public void testParsing3() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		try {
			//this was found by the fuzzying tool
			Query q = pm.newQuery(TestClass.class, "0bd00xLget|0b1this<=.(or\\");
			q.execute();
			fail();
		} catch (JDOUserException e) {
			//good
			assertTrue(e.getMessage().contains("Cannot parse query"));
		}
	}
	
	@Test
	public void testParsing4() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		try {
			//this was found by the fuzzying tool
			Query q = pm.newQuery(TestClass.class, "this");
			q.execute();
			fail();
		} catch (JDOUserException e) {
			//good
			assertTrue(e.getMessage().contains("unexpected end at position"));
		}
	}
	
	@Test
	public void testParsing5() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		try {
			//this was found by the fuzzying tool
			Query q = pm.newQuery(TestClass.class, "d<=::0!& .contains(");
			q.execute();
			fail();
		} catch (JDOUserException e) {
			//good
			assertTrue(e.getMessage().contains("Parsing error"));
		}
	}

	@Test
	public void testParsing6() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		//this was found by the fuzzying tool
		Query q = pm.newQuery(TestClass.class, " ");
		q.execute();
	}

	@Test
	public void testParsing7() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		try {
			//this was found by the fuzzying tool
			Query q = pm.newQuery(TestClass.class, "containsand!='get&0x^;f<=! isEmpty>this'");
			q.execute();
			fail();
		} catch (JDOUserException e) {
			//good
			e.printStackTrace();
			assertTrue(e.getMessage().contains("Parsing error"));
		}
	}

	@Test
	public void testParsing8() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		try {
			//this was found by the fuzzying tool
			Query q = pm.newQuery(TestClass.class, "0!=::|~!=");
			q.execute();
			fail();
		} catch (JDOUserException e) {
			//good
			assertTrue(e.getMessage().contains("Parsing error"));
		}
	}

	@Test
	public void testParsing9() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		try {
			//this was found by the fuzzying tool
			Query q = pm.newQuery(TestClass.class, "_float<=_bool");
			q.execute();
			fail();
		} catch (JDOUserException e) {
			//good
			assertTrue(e.getMessage().contains("annot compare"));
		}
	}

	@Test
	public void testParsing10() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		try {
			//this was found by the fuzzying tool
			Query q = pm.newQuery(TestClass.class, "_bool<=0");
			q.execute();
			fail();
		} catch (JDOUserException e) {
			//good
			assertTrue(e.getMessage().contains("annot compare"));
		}
	}

	@Test
	public void testParsing11() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		try {
			//this was found by the fuzzying tool
			Query q = pm.newQuery(TestClass.class, ":_long0x!=_bool");
			q.execute(0);
			fail();
		} catch (JDOUserException e) {
			//good
			assertTrue(e.getMessage().contains("ncomparable types"));
		}
	}

	@Test
	public void testParsing12() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		try {
			//this was found by the fuzzying tool
			Query q = pm.newQuery(TestClass.class, ":my_str.substring(1,3)=='xx'");
			q.execute("Hello");
			fail();
		} catch (JDOUserException e) {
			//good
			assertTrue(e.getMessage(), e.getMessage().contains("ate binding"));
		}
	}

	@Test
	public void testParsing13() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		try {
			//this was found by the fuzzying tool
			Query q = pm.newQuery(TestClass.class, "(:my_str).substring(1,3)=='xx'");
			q.execute("Hello");
			fail();
		} catch (JDOUserException e) {
			//good
			//TODO maybe this should actually work??
			//assertTrue(e.getMessage(), e.getMessage().contains("ate binding"));
		}
	}

}

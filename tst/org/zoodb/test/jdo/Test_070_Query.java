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
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.jdo.Extent;
import javax.jdo.JDOFatalUserException;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.test.testutil.TestTools;

public class Test_070_Query {

	@BeforeClass
	public static void setUp() {
        TestTools.removeDb();
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
	}

	@Before
	public void before() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        pm.newQuery(TestClass.class).deletePersistentAll();
        
        TestClass tc1 = new TestClass();
        tc1.setData(1, false, 'c', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
        		-1.1f, 35);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(12, false, 'd', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
        		-0.1f, 34);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(123, false, 'e', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
        		0.1f, 3.0);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(1234, false, 'f', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
        		1.1f, -0.01);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(12345, false, 'g', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
        		11.1f, -35);
        pm.makePersistent(tc1);
        
        pm.currentTransaction().commit();
        TestTools.closePM();;
	}
	
	@Test
	public void testQueryOnWrongClass() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        try {
            pm.newQuery(String.class);
            fail();
        } catch (JDOUserException e) {
            //bound to fail...
        }
        
        try {
            //non persistent class
            pm.newQuery(TestClassSmall.class);
            fail();
        } catch (JDOUserException e) {
            //bound to fail...
        }
        
        TestTools.closePM(pm);
	}
	
	@Test
	public void testQuery() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = pm.newQuery();
		assertEquals(pm, q.getPersistenceManager());
		assertFalse(q.isUnmodifiable());
		
		q.setClass(TestClass.class);
		
		testDeclarative(q);
		testString(q);
		
		TestTools.closePM(pm);
	}

	
	@Test
	public void testQueryOfClass() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = pm.newQuery(TestClass.class);
		assertEquals(pm, q.getPersistenceManager());
		assertFalse(q.isUnmodifiable());
		
		testDeclarative(q);
		testString(q);
		
		TestTools.closePM(pm);
	}

	
	@Test
	public void testQueryOfExtent() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Extent<?> ext = pm.getExtent(TestClass.class);
		Query q = pm.newQuery(ext);
		assertEquals(pm, q.getPersistenceManager());
		assertFalse(q.isUnmodifiable());
		
		
		testDeclarative(q);
		testString(q);
		
		TestTools.closePM(pm);
	}

	@Test
	public void testQueryOfString() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = pm.newQuery("SELECT FROM " + TestClass.class.getName());
		assertEquals(pm, q.getPersistenceManager());
		assertFalse(q.isUnmodifiable());
		
		testDeclarative(q);
		testString(q);
		
		TestTools.closePM(pm);
	}

	@Test
	public void testQueryWithNullArgs() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		try {
			pm.newQuery("");
			fail();
		} catch (NullPointerException e) {
			//good
		}
		try {
			pm.newQuery((String)null);
			fail();
		} catch (NullPointerException e) {
			//good
		}

		Query q = pm.newQuery(TestClass.class, (String)null);
		q = pm.newQuery(TestClass.class, "");
		
		assertEquals(pm, q.getPersistenceManager());
		assertFalse(q.isUnmodifiable());
		
		testDeclarative(q);
		testString(q);
		
		TestTools.closePM(pm);
	}

	@SuppressWarnings("unchecked")
    private void testDeclarative(Query q) {
		q.setFilter("_short == 32000 && _int >= 123");
		Collection<TestClass> r = (Collection<TestClass>) q.execute();
		assertEquals(3, r.size());
        for (TestClass tc: r) {
			assertTrue("int="+tc.getInt(), tc.getInt() >= 123);
		}
		
		q.setFilter("_short == 32000 && _int >= 123 && _int < 12345");
		r = (Collection<TestClass>) q.execute();
		assertEquals(2, r.size());
        for (TestClass tc: r) {
			assertTrue("int="+tc.getInt(), tc.getInt() >= 123);
		}
	}
	
	@SuppressWarnings("unchecked")
    private void testString(Query q) {
		q.setFilter("_int >= 123 && _short == 32000");
		Collection<TestClass> r = (Collection<TestClass>) q.execute();
		assertEquals(3, r.size());
		for (TestClass tc: r) {
			assertTrue("int="+tc.getInt(), tc.getInt() >= 123);
		}
        
        q.setFilter("_int < 12345 && _short == 32000 && _int >= 123");
        r = (Collection<TestClass>) q.execute();
        assertEquals(2, r.size());
        for (TestClass tc: r) {
            assertTrue("int="+tc.getInt(), tc.getInt() >= 123);
        }
	}

    @SuppressWarnings("unchecked")
    @Test
    public void testIterator() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        Query q = pm.newQuery(TestClass.class, "_int < 12345");
        assertEquals(pm, q.getPersistenceManager());
        assertFalse(q.isUnmodifiable());

        Collection<TestClass> r = (Collection<TestClass>) q.execute();
        assertEquals(4, r.size());
        Iterator<TestClass> iter = r.iterator();
        //avoid call to hasNext()
        for (int i = 0; i < 4; i++) {
            iter.next();
        }
        
        try {
            iter.next();
            fail();
        } catch (NoSuchElementException e) {
            //good
        }
        
        assertFalse(iter.hasNext());

        pm.currentTransaction().rollback();
        TestTools.closePM();
    }   
    
    
    /**
     * Test with field value = null/0.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testNullValue() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        TestClass tc = new TestClass();
        //_string == null
        pm.makePersistent(tc);
        
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        Query q = pm.newQuery(TestClass.class, "_string == 'ddd'");
        Collection<TestClass> r = (Collection<TestClass>) q.execute();
        assertEquals(0, r.size());
        q.closeAll();

        q = pm.newQuery(TestClass.class, "_string == null");
        r = (Collection<TestClass>) q.execute();
        assertEquals(1, r.size());
        q.closeAll();

        q = pm.newQuery(TestClass.class, "_int == 0");
        r = (Collection<TestClass>) q.execute();
        assertEquals(1, r.size());
        q.closeAll();

        q = pm.newQuery(TestClass.class, "_string == null && _int == 0");
        r = (Collection<TestClass>) q.execute();
        assertEquals(1, r.size());
        q.closeAll();

        pm.currentTransaction().rollback();
        TestTools.closePM();
    }   
    
    
    @SuppressWarnings("unchecked")
    private void checkQuery(String queryStr, int nRes) {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        Query q = pm.newQuery("SELECT FROM " + TestClass.class.getName() + " WHERE " + queryStr);
        assertEquals(pm, q.getPersistenceManager());
        assertFalse(q.isUnmodifiable());

        Collection<TestClass> r = (Collection<TestClass>) q.execute();
        assertEquals(nRes, r.size());
        for (TestClass tc: r) {
            //just check existence
            assertTrue(tc.getInt() >= 1);
        }

        pm.currentTransaction().rollback();
        TestTools.closePM();
	}

    @Test
    public void testSyntaxBraces1() {
        checkQuery("_int < 12345", 4);
        checkQuery("(_int < 12345)", 4);
        checkQuery("((_int < 12345))", 4);
        checkQuery("(((_int < 12345)))", 4);
    }   
    
    @Test
    public void testSyntaxBraces2() {
        checkQuery("_int < 12345 && _short == 32000", 4);
        checkQuery("(_int < 12345 && _short == 32000)", 4);
        checkQuery("(((_int < 12345) && ((_short == 32000))))", 4);
    }   
    
    @Test
    public void testSyntaxBraces3() {
        checkQuery("_int < 12345 && _short == 32000 && _int >= 123", 2);
        checkQuery("((_int < 12345) && (((_short == 32000) && (_int >= 123))))", 2);
        checkQuery("((((_int < 12345)) && _short == 32000) && (_int >= 123))", 2);
    }   
    
	@SuppressWarnings("unchecked")
    @Test
	public void testSyntaxBracesAndOperators() {
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
            assertTrue("int="+tc.getInt(), tc.getInt() >= 123);
        }

        //again with ""
        q.setFilter("_int < 12345 && (_short == 32000 || _string == \"xyz\") && _int >= 123");
        r = (Collection<TestClass>) q.execute();
        assertEquals(2, r.size());
        for (TestClass tc: r) {
            assertTrue("int="+tc.getInt(), tc.getInt() >= 123);
        }
        TestTools.closePM(pm);
	}

	
	@SuppressWarnings("unchecked")
    @Test
	public void testSpaces() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        //TABS
        Query q = pm.newQuery("SELECT	FROM " + TestClass.class.getName());
        assertEquals(pm, q.getPersistenceManager());
        assertFalse(q.isUnmodifiable());

        Collection<TestClass> r;
        
        //no spaces
        q.setFilter("_int<12345&&(_short==32000||_string=='xyz')&&_int>=123");
        r = (Collection<TestClass>) q.execute();
        assertEquals(2, r.size());
        for (TestClass tc: r) {
            assertTrue("int="+tc.getInt(), tc.getInt() >= 123);
        }

        //tabs i.o. spaces
        q.setFilter("	_int	<	12345	&&	(	_short	==	32000	||	_string	==	'xyz'	)" +
        		"	&&	_int	>=	123");
        r = (Collection<TestClass>) q.execute();
        assertEquals(2, r.size());
        for (TestClass tc: r) {
            assertTrue("int="+tc.getInt(), tc.getInt() >= 123);
        }

        //cr/lf (nl) \r \f \n \t
        q.setFilter("	_int\r<\f12345\n&&\t(	_short	==	32000	||	_string	==	'xyz'	)" +
        		"	&&	_int	>=	123");
        r = (Collection<TestClass>) q.execute();
        assertEquals(2, r.size());
        for (TestClass tc: r) {
            assertTrue("int="+tc.getInt(), tc.getInt() >= 123);
        }

        TestTools.closePM(pm);
	}

	
	/**
	 * Queries used to fail if the string ended with true/false.
	 */
	@Test
	public void testBooleanEnding() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q1 = pm.newQuery(TestClass.class, "_bool == false");
		q1.execute();

		Query q2 = pm.newQuery(TestClass.class, "_bool == true");
		q2.execute();

		TestTools.closePM();
	}
	

	@Test
	public void testNewQueryExtentFilter() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Extent<?> ext = pm.getExtent(TestClass.class);
		Query q = pm.newQuery(ext, "_int >= 123");
		Collection<?> c = (Collection<?>) q.execute();
		assertEquals(3, c.size());
		
        TestTools.closePM(pm);
	}
		
	@SuppressWarnings("unchecked")
    @Test
	public void testNOT() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        Query q = pm.newQuery("SELECT FROM " + TestClass.class.getName());

        Collection<TestClass> r;
        
        q.setFilter("_int != 123");
        r = (Collection<TestClass>) q.execute();
        assertEquals(4, r.size());
        for (TestClass tc: r) {
            assertTrue("int="+tc.getInt(), tc.getInt() != 123);
        }

        q.setFilter("_int < 12345 && (_int != 123)");
        r = (Collection<TestClass>) q.execute();
        assertEquals(3, r.size());
        for (TestClass tc: r) {
            assertTrue("int="+tc.getInt(), tc.getInt() != 123);
        }

        q.setFilter("_int < 12345 && !(_int == 123)");
        r = (Collection<TestClass>) q.execute();
        for (TestClass tc: r) {
            assertTrue("int="+tc.getInt(), tc.getInt() < 12345);
            assertTrue("int="+tc.getInt(), tc.getInt() != 123);
        }
        assertEquals(3, r.size());


        q.setFilter("_int < 12345 && !(_int != 123)");
        r = (Collection<TestClass>) q.execute();
        assertEquals(1, r.size());
        for (TestClass tc: r) {
            assertTrue("int="+tc.getInt(), tc.getInt() == 123);
        }

         //negation on dual-term &&
        q.setFilter("!(_int > 123 && !(_int >= 12345))");
        r = (Collection<TestClass>) q.execute();
        for (TestClass tc: r) {
            assertTrue("int="+tc.getInt(), tc.getInt() != 1234);
        }
        assertEquals(4, r.size());

        //negation on dual-term ||
        q.setFilter("!(_int < 123 || _int >= 1234)");
        r = (Collection<TestClass>) q.execute();
        assertEquals(1, r.size());
        for (TestClass tc: r) {
            assertTrue("int="+tc.getInt(), tc.getInt() == 123);
        }

        //test with brackets
        q.setFilter("(!!(_int < 12345)) && !(!!(_int == 123))");
        r = (Collection<TestClass>) q.execute();
        assertEquals(3, r.size());
        for (TestClass tc: r) {
            assertTrue("int="+tc.getInt(), tc.getInt() < 12345);
            assertTrue("int="+tc.getInt(), tc.getInt() != 123);
        }

        //test with spaces
        q.setFilter("( ! ! (_int < 12345)) && ! ( ! ! (_int == 123))");
        r = (Collection<TestClass>) q.execute();
        assertEquals(3, r.size());
        for (TestClass tc: r) {
            assertTrue("int="+tc.getInt(), tc.getInt() < 12345);
            assertTrue("int="+tc.getInt(), tc.getInt() != 123);
        }

        TestTools.closePM(pm);
	}

	
	/**
	 * This used to fail, but only when using indices.
	 */
	@SuppressWarnings("unchecked")
    @Test
	public void testOrBugWhenUsingIndices() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        Query q = pm.newQuery("SELECT FROM " + TestClass.class.getName());

        Collection<TestClass> r;
        
        q.setFilter("(_int <= 123 || (_int >= 12345))");
        r = (Collection<TestClass>) q.execute();
        for (TestClass tc: r) {
            assertTrue("int="+tc.getInt(), tc.getInt() != 1234);
        }
        assertEquals(4, r.size());
        
        //negation on dual-term &&
        q.setFilter("!(_int > 123 && !(_int >= 12345))");
        r = (Collection<TestClass>) q.execute();
        for (TestClass tc: r) {
            assertTrue("int="+tc.getInt(), tc.getInt() != 1234);
        }
        assertEquals(4, r.size());

        //negation on dual-term ||
        q.setFilter("!(_int < 123 || _int >= 1234)");
        r = (Collection<TestClass>) q.execute();
        assertEquals(1, r.size());
        for (TestClass tc: r) {
            assertTrue("int="+tc.getInt(), tc.getInt() == 123);
        }

        TestTools.closePM(pm);
	}

	
	/**
	 * This tests the OR splitter, which splits a query that contains an OR into multiple OR-free
	 * queries.
	 * However the splitter is only used when indices are used.
	 */
	@SuppressWarnings("unchecked")
    @Test
	public void testOrWhenUsingIndices() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        Query q = pm.newQuery("SELECT FROM " + TestClass.class.getName());

        Collection<TestClass> r;
        
        q.setFilter("_int < 123 || _int >= 1234");
        r = (Collection<TestClass>) q.execute();
        assertEquals(4, r.size());
        for (TestClass tc: r) {
            assertTrue("int="+tc.getInt(), tc.getInt() != 123);
        }

        q.setFilter("(_int <= 123 || (_int >= 12345))");
        r = (Collection<TestClass>) q.execute();
        for (TestClass tc: r) {
            assertTrue("int="+tc.getInt(), tc.getInt() != 1234);
        }
        assertEquals(4, r.size());
        
        q.setFilter("(_int == 123 || _int == 1234) && (_int > 12345 || _int <= 123))");
        r = (Collection<TestClass>) q.execute();
        for (TestClass tc: r) {
            assertTrue("int="+tc.getInt(), tc.getInt() == 123);
        }
        assertEquals(1, r.size());

        q.setFilter("_int == 123 || _int == 123");
        r = (Collection<TestClass>) q.execute();
        for (TestClass tc: r) {
            assertTrue("int="+tc.getInt(), tc.getInt() == 123);
        }
        assertEquals(1, r.size());

        q.setFilter("(_int == 123 || _int == 123) || (_int == 123)");
        r = (Collection<TestClass>) q.execute();
        for (TestClass tc: r) {
            assertTrue("int="+tc.getInt(), tc.getInt() == 123);
        }
        assertEquals(1, r.size());

        q.setFilter("(_int == 123 || _int == 1234) || (_int > 12345)");
        r = (Collection<TestClass>) q.execute();
        for (TestClass tc: r) {
            assertTrue("int="+tc.getInt(), tc.getInt() != 1);
            assertTrue("int="+tc.getInt(), tc.getInt() != 12);
            assertTrue("int="+tc.getInt(), tc.getInt() != 12345);
        }
        assertEquals(2, r.size());

        q.setFilter("(_int == 123 || _int == 1234) || (_int > 12345 || _int <= 1))");
        r = (Collection<TestClass>) q.execute();
        for (TestClass tc: r) {
            assertTrue("int="+tc.getInt(), tc.getInt() != 12);
            assertTrue("int="+tc.getInt(), tc.getInt() != 12345);
        }
        assertEquals(3, r.size());

        TestTools.closePM(pm);
	}

	@Test
	public void testParsingErrors() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        Query q = pm.newQuery("SELECT FROM " + TestClass.class.getName());

        //bad logical operator (i.e. OR instead of ||)
        checkFilterFail(q, "_int <= 123 OR _int >= 12345");
        
        //bad field name
        checkFilterFail(q, "_int <= 123 || %% >= 12345");
        
        //bad value
        checkFilterFail(q, "_int <= null || _int >= 12345");

        //bad value #2
        checkFilterFail(q, "_int <= 'hallo' || _int >= 12345");
    	
        //bad value #3
        checkFilterFail(q, "_bool == falsch || _int >= 12345");
    	
        //bad value #3
        checkFilterFail(q, "_string <= 123 || _int >= 12345");

        //bad trailing slashes
        checkFilterFail(q, "_string == '\\'");

        //bad trailing slashes
        checkFilterFail(q, "_string == 'C:\\\\Windows\\'");

        //single =
        checkFilterFail(q, "_int = 1");

        TestTools.closePM(pm);
	}
	
	private void checkFilterFail(Query q, String filter) {
		try {
			q.setFilter(filter);
			q.execute();
			fail();
		} catch (JDOUserException e) {
			//good
		}
	}
	
	@Test
	public void testHex() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = pm.newQuery("SELECT FROM " + TestClass.class.getName());
		Collection<?> c;
		
		q.setFilter("_int == 0x1 && _short == 0x7D00 && _long == 0x499602D2 && _byte == 0x7F");
		c = (Collection<?>) q.execute();
		assertEquals(1, c.size());

		TestTools.closePM(pm);
	}
	

	/**
	 * This used to fail, but only when using indices.
	 */
	@SuppressWarnings("unchecked")
    @Test
	public void testNegativeValues() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        Query q = pm.newQuery(TestClass.class);

        Collection<TestClass> r;
        
        q.setFilter("_double == -35f");
        r = (Collection<TestClass>) q.execute();
        for (TestClass tc: r) {
            assertTrue("int = " + tc.getInt(), tc.getDouble() == -35);
        }
        assertEquals(1, r.size());
        
        q.setFilter("_double != -35f");
        r = (Collection<TestClass>) q.execute();
        for (TestClass tc: r) {
            assertTrue("int = " + tc.getInt(), tc.getDouble() != -35);
        }
        assertEquals(4, r.size());
        
        TestTools.closePM(pm);
	}

	/**
	 * In issue #91, a query would not check if the PM is still
	 * open if the query was executed on an indexed attribute.
	 */
	@Test
	public void testQueryOnClosedPM_Issue91_92() {
		TestTools.defineSchema(TestClassTiny.class, TestClassTiny2.class);
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		TestClassTiny t11 = new TestClassTiny(11, 11);
		pm.makePersistent(t11);
		TestClassTiny t12 = new TestClassTiny(12, 12);
		pm.makePersistent(t12);
		TestClassTiny2 t22 = new TestClassTiny2(21, 21, 21, 21);
		pm.makePersistent(t22);
		pm.currentTransaction().commit();

		TestTools.closePM(pm);

		testExtentOnClosedPM_Issue91(TestClassTiny.class, "", false);
		testExtentOnClosedPM_Issue91(TestClassTiny2.class, "", false);
		testExtentOnClosedPM_Issue91(TestClass.class, "_double == -35f", false);
		
		testExtentOnClosedPM_Issue91(TestClassTiny.class, "", true);
		testExtentOnClosedPM_Issue91(TestClassTiny2.class, "", true);
		testExtentOnClosedPM_Issue91(TestClass.class, "_double == -35f", true);
	}


	/**
	 * 
	 * @param cls
	 * @param filter
	 * @param alwaysFail JDO specifies that calling hasNext()/next on
	 * a closed query should behave as if the end of the result has been
	 * reached. This can be emulated by setting 'alwaysFails' to 'false'.
	 * ZooDB also support a mode where accessing a query outside of a transaction 
	 * always fails with a JDOUserException. This can be emulated with
	 * 'alwaysFails' to 'false'.
	 */
	@SuppressWarnings("unchecked")
	private <T> void testExtentOnClosedPM_Issue91(Class<T> cls,	String filter, 
			boolean alwaysFail) {
		ZooJdoProperties props = TestTools.getProps();
		props.setZooFailOnEmptyQueries(alwaysFail);
        PersistenceManager pm = TestTools.openPM(props);
        pm.currentTransaction().begin();

        Query q1 = pm.newQuery(cls);
        Query q2 = pm.newQuery(cls, filter);
        Query q3 = pm.newQuery(cls, filter);
        Collection<T> c3 = (Collection<T>) q3.execute();
        Iterator<T> i3 = c3.iterator();

        pm.currentTransaction().commit();
        
        try {
        	q1.execute();
        	fail();
        } catch (JDOUserException e) {
        	assertTrue(e.getMessage(), e.getMessage().contains("not active"));
        }

        try {
        	q2.execute();
        	fail();
        } catch (JDOUserException e) {
        	assertTrue(e.getMessage(), e.getMessage().contains("not active"));
        }

        //iterator()
        if (alwaysFail) {
	        try {
	        	c3.iterator();
	        	fail();
	        } catch (JDOUserException e) {
	        	assertTrue(e.getMessage(), e.getMessage().contains("not active"));
	        }
        } else {
        	//TODO see outcome of https://issues.apache.org/jira/browse/JDO-735
        	System.err.println("FIXME: Test_070_Query.testQueryOnClosedPM_Issue91();");
        	assertFalse(c3.iterator().hasNext());
        }

        //isEmpty()
        if (alwaysFail) {
	        try {
	        	c3.isEmpty();
	        	fail();
	        } catch (JDOUserException e) {
	        	assertTrue(e.getMessage(), e.getMessage().contains("not active"));
	        }
        } else {
        	assertTrue(c3.isEmpty());
        }

        //hasNext()
        if (alwaysFail) {
	        try {
	        	i3.hasNext();
	        	fail();
	        } catch (JDOUserException e) {
	        	assertTrue(e.getMessage(), e.getMessage().contains("closed"));
	//        	assertTrue(e.getMessage(), e.getMessage().contains("not active"));
	        }
		} else {
			assertFalse(i3.hasNext());
		}
			
		//next()	
        if (alwaysFail) {
	        try {
	        	i3.next();
	        	fail();
	        } catch (JDOUserException e) {
	        	assertTrue(e.getMessage(), e.getMessage().contains("closed"));
	//        	assertTrue(e.getMessage(), e.getMessage().contains("not active"));
	        }
		} else {
	        try {
	        	i3.next();
	        	fail();
	        } catch (NoSuchElementException e) {
	        	//good
	        }
		}
			
			

        try {
        	pm.newQuery(TestClass.class);
        	fail();
        } catch (JDOUserException e) {
        	assertTrue(e.getMessage(), e.getMessage().contains("not active"));
        }
       
       	//execution should work now:
        pm.currentTransaction().setNontransactionalRead(true);
    	q1.execute();
    	q2.execute();
    	c3 = (Collection<T>) q3.execute();
    	i3 = c3.iterator();
    	i3.next();

    	//Now, try everything with closed session
    	pm.close();
    	
        try {
        	q1.execute();
        	fail();
        } catch (JDOUserException e) {
        	assertTrue(e.getMessage(), e.getMessage().contains("closed"));
        }

        try {
        	q2.execute();
        	fail();
        } catch (JDOUserException e) {
        	assertTrue(e.getMessage(), e.getMessage().contains("closed"));
        }
    	
        try {
        	//TODO see outcome of https://issues.apache.org/jira/browse/JDO-735
        	System.err.println("FIXME: Test_070_Query.testQueryOnClosedPM_Issue91();");
        	c3.iterator();
        	fail();
        } catch (JDOUserException e) {
        	assertTrue(e.getMessage(), e.getMessage().contains("closed"));
        }

    	//TODO !!!!
    	//TODO !!!!
    	//TODO !!!!

        //TODO see outcome of https://issues.apache.org/jira/browse/JDO-735
        System.err.println("FIXME: Test_070_Query.testQueryOnClosedPM_Issue91();");
        if (alwaysFail) {
        	try {
              	i3.hasNext();
              	fail();
        	} catch (JDOUserException e) {
        		//good
            	assertTrue(e.getMessage(), e.getMessage().contains("closed"));
        	}
        } else {
          	assertFalse(i3.hasNext());
        }
      	
        if (alwaysFail) {
        	try {
        		//TODO see outcome of https://issues.apache.org/jira/browse/JDO-735
        		System.err.println("FIXME: Test_070_Query.testQueryOnClosedPM_Issue91();");
        		i3.next();
        		fail();
        	} catch (JDOUserException e) {
        		assertTrue(e.getMessage(), e.getMessage().contains("closed"));
        	}
        } else {
        	try {
        		//TODO see outcome of https://issues.apache.org/jira/browse/JDO-735
        		System.err.println("FIXME: Test_070_Query.testQueryOnClosedPM_Issue91();");
        		i3.next();
        		fail();
        	} catch (NoSuchElementException e) {
        		//good
        	}
        }

      	TestTools.closePM(pm);
	}


	@After
	public void afterTest() {
		TestTools.closePM();
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
	}
	
}

package org.zoodb.test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.util.TestTools;

public class Test_070_Query {

	@BeforeClass
	public static void setUp() {
        TestTools.removeDb();
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);

		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		TestClass tc1 = new TestClass();
		tc1.setData(1, false, 'c', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2});
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(12, false, 'd', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2});
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(123, false, 'e', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2});
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(1234, false, 'f', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2});
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(12345, false, 'g', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2});
		pm.makePersistent(tc1);
		
		pm.currentTransaction().commit();
		TestTools.closePM();;
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

	@SuppressWarnings("unchecked")
    private void testDeclarative(Query q) {
		q.setFilter("_short == 32000 && _int >= 123");
		List<TestClass> r = (List<TestClass>) q.execute();
		assertEquals(3, r.size());
        for (TestClass tc: r) {
			assertTrue("int="+tc.getInt(), tc.getInt() >= 123);
		}
		
		q.setFilter("_short == 32000 && _int >= 123 && _int < 12345");
		r = (List<TestClass>) q.execute();
		assertEquals(2, r.size());
        for (TestClass tc: r) {
			assertTrue("int="+tc.getInt(), tc.getInt() >= 123);
		}
	}
	
	@SuppressWarnings("unchecked")
    private void testString(Query q) {
		q.setFilter("_int >= 123 && _short == 32000");
		List<TestClass> r = (List<TestClass>) q.execute();
		assertEquals(3, r.size());
		for (TestClass tc: r) {
			assertTrue("int="+tc.getInt(), tc.getInt() >= 123);
		}
        
        q.setFilter("_int < 12345 && _short == 32000 && _int >= 123");
        r = (List<TestClass>) q.execute();
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

        List<TestClass> r = (List<TestClass>) q.execute();
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
    
    
    @SuppressWarnings("unchecked")
    private void checkQuery(String queryStr, int nRes) {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        Query q = pm.newQuery("SELECT FROM " + TestClass.class.getName() + " WHERE " + queryStr);
        assertEquals(pm, q.getPersistenceManager());
        assertFalse(q.isUnmodifiable());

        List<TestClass> r = (List<TestClass>) q.execute();
        assertEquals(nRes, r.size());
        for (TestClass tc: r) {
            //just check existence
            assertTrue(tc.getInt() >= 1);
        }

        pm.currentTransaction().rollback();
        TestTools.closePM();
	}

    @Test
    public void testSyntaxBrackets1() {
        checkQuery("_int < 12345", 4);
        checkQuery("(_int < 12345)", 4);
        checkQuery("((_int < 12345))", 4);
        checkQuery("(((_int < 12345)))", 4);
    }   
    
    @Test
    public void testSyntaxBrackets2() {
        checkQuery("_int < 12345 && _short == 32000", 4);
        checkQuery("(_int < 12345 && _short == 32000)", 4);
        checkQuery("(((_int < 12345) && ((_short == 32000))))", 4);
    }   
    
    @Test
    public void testSyntaxBrackets3() {
        checkQuery("_int < 12345 && _short == 32000 && _int >= 123", 2);
        checkQuery("((_int < 12345) && (((_short == 32000) && (_int >= 123))))", 2);
        checkQuery("((((_int < 12345)) && _short == 32000) && (_int >= 123))", 2);
    }   
    
	@SuppressWarnings("unchecked")
    @Test
	public void testSyntax() {
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
		
	
	@After
	public void afterTest() {
		TestTools.closePM();
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
	}
	
}

package org.zoodb.test;

import static junit.framework.Assert.*;

import java.util.List;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class Test_070_Query {

	private static final String DB_NAME = "TestDb";
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb(DB_NAME);
		TestTools.defineSchema(DB_NAME, TestClass.class);

		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		TestClass tc1 = new TestClass();
		tc1.setData(1, false, 'c', (byte)127, (short)32000, 1234567890l, "xyz", new byte[]{1,2});
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(12, false, 'd', (byte)127, (short)32000, 1234567890l, "xyz", new byte[]{1,2});
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(123, false, 'e', (byte)127, (short)32000, 1234567890l, "xyz", new byte[]{1,2});
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(1234, false, 'f', (byte)127, (short)32000, 1234567890l, "xyz", new byte[]{1,2});
		pm.makePersistent(tc1);
		tc1 = new TestClass();
		tc1.setData(12345, false, 'g', (byte)127, (short)32000, 1234567890l, "xyz", new byte[]{1,2});
		pm.makePersistent(tc1);
		
		pm.currentTransaction().commit();
		TestTools.closePM();;
	}

	@Test
	public void testQuery() {
		System.out.println("Testing Query()");
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
		System.out.println("Testing Query(Class)");
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
		System.out.println("Testing Query(Extent)");
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
		System.out.println("Testing Query(Extent)");
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = pm.newQuery("SELECT FROM " + TestClass.class.getName());
		assertEquals(pm, q.getPersistenceManager());
		assertFalse(q.isUnmodifiable());
		
		testDeclarative(q);
		testString(q);
		
		TestTools.closePM(pm);
	}

	private void testDeclarative(Query q) {
		q.setFilter("_short == 32000 && _int >= 123");
		List r = (List) q.execute();
		assertEquals(3, r.size());
		for (Object o: r) {
			TestClass tc = (TestClass) o;
			assertTrue("int="+tc.getInt(), tc.getInt() >= 123);
		}
		
		q.setFilter("_short == 32000 && _int >= 123 && _int < 12345");
		r = (List) q.execute();
		assertEquals(2, r.size());
		for (Object o: r) {
			TestClass tc = (TestClass) o;
			assertTrue("int="+tc.getInt(), tc.getInt() >= 123);
		}
	}
	
	private void testString(Query q) {
		q.setFilter("_int >= 123 && _short == 32000");
		List r = (List) q.execute();
		assertEquals(3, r.size());
		for (Object o: r) {
			TestClass tc = (TestClass) o;
			assertTrue("int="+tc.getInt(), tc.getInt() >= 123);
		}
		
		q.setFilter("_int < 12345 && _short == 32000 && _int >= 123");
		r = (List) q.execute();
		assertEquals(2, r.size());
		for (Object o: r) {
			TestClass tc = (TestClass) o;
			assertTrue("int="+tc.getInt(), tc.getInt() >= 123);
		}
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

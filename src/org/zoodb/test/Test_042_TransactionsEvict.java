package org.zoodb.test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.util.TestTools;

public class Test_042_TransactionsEvict {

	private static final String DB_NAME = "TestDb";
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb(DB_NAME);
		TestTools.defineSchema(TestClass.class);
	}

	@Before
	public void before() {
		TestTools.dropInstances(TestClass.class);
	}
	
	@Test
	public void testNoEvictNew() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		tc.setInt(5);
		pm.makePersistent(tc);
		
		pm.evictAll();
		pm.evictAll(tc);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		assertEquals(5, tc.getInt());
		assertTrue(JDOHelper.isPersistent(tc));
		tc.setInt(55);
		assertEquals(55, tc.getInt());
		
		//does commit work?
		pm.deletePersistent(tc);

		pm.currentTransaction().commit();
		pm.close();
	}
	
	@Test
	public void testNoEvict() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		tc.setInt(55);
		pm.makePersistent(tc);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		
		pm.evictAll();
		pm.evictAll(tc);
		
		assertEquals(55, tc.getInt());
		tc.setInt(555);
		assertEquals(555, tc.getInt());
		assertTrue(JDOHelper.isPersistent(tc));
		
		//does commit work?
		pm.deletePersistent(tc);

		pm.currentTransaction().commit();
		pm.close();
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

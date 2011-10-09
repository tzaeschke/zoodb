package org.zoodb.test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

import java.util.Collection;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.util.TestTools;

public class Test_061_ExtentDeletion {

	@BeforeClass
	public static void setUp() {
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class, TestClassTiny.class, TestClassTiny2.class);
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
	}
	
	@Before
	public void beforeTest() {
	    PersistenceManager pm = TestTools.openPM();
	    pm.currentTransaction().begin();
        pm.newQuery(pm.getExtent(TestClass.class)).deletePersistentAll();
        pm.newQuery(pm.getExtent(TestClassTiny.class)).deletePersistentAll();
        pm.newQuery(pm.getExtent(TestClassTiny2.class)).deletePersistentAll();
        pm.currentTransaction().commit();
        TestTools.closePM();
	}

	@After
	public void afterTest() {
		TestTools.closePM();
	}
	
	private void count(PersistenceManager pm, Class<?> cls, int nExp) {
		int nExt = 0;
		for (Object o: pm.getExtent(cls, false)) {
			assertNotNull(o);
			nExt++;
		}
		
		int nQ = 0;
		Collection<?> c = (Collection<?>) pm.newQuery(pm.getExtent(cls, false)).execute();
		for (Object o: c) {
			assertNotNull(o);
			nQ++;
		}
		
		assertEquals(nExp, nExt);
		assertEquals(nExp, nQ);
	}
	
	private void checkCount(PersistenceManager pm, int nTC, int nTCT, int nTCT2) {
		count( pm, TestClass.class, nTC);
		count( pm, TestClassTiny.class, nTCT);
		count( pm, TestClassTiny2.class, nTCT2);
	}
	
	@Test
	public void testExtentDeletionNoHierarchyNoFilter() {
		test(false, false);
	}
	
	
	@Test
	public void testExtentDeletionHierarchyNoFilter() {
		test(true, false);
	}
	
	@Test
	public void testExtentDeletionNoHierarchyFilter() {
		test(false, true);
	}
	
	
	@Test
	public void testExtentDeletionHierarchyFilter() {
		test(true, true);
	}
	
	private void test(boolean hierarchy, boolean filter) {
		int nTC2 = hierarchy ? 0 : 1;
		
		PersistenceManager pm = TestTools.openPM();
		pm.setIgnoreCache(false);
		pm.currentTransaction().begin();

		//create
		pm.makePersistent(new TestClass());
		pm.makePersistent(new TestClassTiny());
		pm.makePersistent(new TestClassTiny2());
		checkCount(pm, 1, 1, 1);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		checkCount(pm, 1, 1, 1);
		//delete
		Extent<?> ext = pm.getExtent(TestClassTiny.class, hierarchy);
		if (filter) {
			pm.newQuery(ext, "_int < 1000000").deletePersistentAll();
		} else {
			pm.newQuery(ext).deletePersistentAll();
		}
		checkCount(pm, 1, 0, nTC2);

		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();
		//check successful rollback
		checkCount(pm, 1, 1, 1);
		//delete again
		ext = pm.getExtent(TestClassTiny.class, hierarchy);
		if (filter) {
			pm.newQuery(ext, "_int < 1000000").deletePersistentAll();
		} else {
			pm.newQuery(ext).deletePersistentAll();
		}
		checkCount(pm, 1, 0, nTC2);

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		checkCount(pm, 1, 0, nTC2);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
}

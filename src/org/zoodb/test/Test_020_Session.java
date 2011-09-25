package org.zoodb.test;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.test.util.TestTools;

public class Test_020_Session {
	
	private static final String DB_NAME = "TestDb";
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb(DB_NAME);
	}

	@Test
	public void testCreateAndCloseSession() {
		if (true) fail("Need to fix multi-PMF");

		ZooJdoProperties props = new ZooJdoProperties(DB_NAME);
		PersistenceManagerFactory pmf1 = 
			JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm11 = pmf1.getPersistenceManager();

		PersistenceManagerFactory pmf2 = 
			JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm21 = pmf2.getPersistenceManager();
		
		//should have returned different pm's
		assertFalse(pm21 == pm11);

		PersistenceManager pm12 = pmf1.getPersistenceManager();
		//should never return same pm (JDO spec 2.2/11.2)
		assertTrue(pm12 != pm11);

		try {
			pmf1.close();
			fail();
		} catch (JDOUserException e) {
			//good, there are still open session!
		}
		
		assertFalse(pm11.isClosed());
		assertFalse(pm12.isClosed());
		pm11.close();
		pm12.close();
		assertTrue(pm11.isClosed());
		assertTrue(pm12.isClosed());
	
		assertFalse(pm21.isClosed());
		pm21.close();
		assertTrue(pm21.isClosed());

		pmf1.close();
		pmf2.close();
		
		try {
			pmf1.getPersistenceManager();
			fail();
		} catch (JDOUserException e) {
			//good, it's closed!
		}
		
		try {
			pmf1.setConnectionURL("xyz");
			fail();
		} catch (JDOUserException e) {
			//good, there are still open session!
		}

	}
	
	@Test
	public void testIgnoreCacheSettings() {
		ZooJdoProperties props = new ZooJdoProperties(DB_NAME);
		PersistenceManagerFactory pmf = 
			JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm = pmf.getPersistenceManager();
		assertFalse(pmf.getIgnoreCache());
		assertFalse(pm.getIgnoreCache());
		
		pm.setIgnoreCache(true);
		assertFalse(pmf.getIgnoreCache());
		assertTrue(pm.getIgnoreCache());
		pm.close();
		
		
		pmf.setIgnoreCache(true);
		assertTrue(pmf.getIgnoreCache());
		pm = pmf.getPersistenceManager();
		assertTrue(pm.getIgnoreCache());
		pm.close();
		pmf.close();
		
		
		props.setIgnoreCache(true);
		pmf = JDOHelper.getPersistenceManagerFactory(props);
		pm = pmf.getPersistenceManager();
		assertTrue(pmf.getIgnoreCache());
		assertTrue(pm.getIgnoreCache());

		pm.close();
		pmf.close();
	}
	
	/**
	 * Only one process should be allowed to connect to a database.
	 * But should a single process be allowed to create multiple PMFs?
	 */
	@Test
	public void testDualSession() {
		//TODO
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb(DB_NAME);
	}
}

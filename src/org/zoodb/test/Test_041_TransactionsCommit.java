package org.zoodb.test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.fail;

import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.test.util.TestTools;

public class Test_041_TransactionsCommit {

	private static final String DB_NAME = "TestDb";
	
	private PersistenceManager pm;
	private PersistenceManagerFactory pmf;
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb(DB_NAME);
		TestTools.defineSchema(TestClass.class);
	}

	@Test
	public void testDoubleDelete() {
		Properties props = new ZooJdoProperties(DB_NAME);
		pmf = JDOHelper.getPersistenceManagerFactory(props);
		pm = pmf.getPersistenceManager();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		pm.makePersistent(tc);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		pm.deletePersistent(tc);
		try {
			pm.deletePersistent(tc);
		} catch (JDOUserException e) {
			//good
		}
		try {
			pm.refresh(tc);
		} catch (JDOUserException e) {
			//good
		}
		pm.currentTransaction().commit();
		pm.close();
	}
	
	@Test
	public void testNonPersistentObject() {
		Properties props = new ZooJdoProperties(DB_NAME);
		pmf = JDOHelper.getPersistenceManagerFactory(props);
		pm = pmf.getPersistenceManager();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		
		try {
			pm.deletePersistent(tc);
		} catch (JDOUserException e) {
			//good
		}

		try {
			pm.refresh(tc);
		} catch (JDOUserException e) {
			//good
		}

		pm.currentTransaction().commit();
		pm.close();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testTransactionBoundaries() {
		Properties props = new ZooJdoProperties(DB_NAME);
		pmf = JDOHelper.getPersistenceManagerFactory(props);
		pm = pmf.getPersistenceManager();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		tc.setInt(5);
		pm.makePersistent(tc);
		TestClass tcD = new TestClass();
		tcD.setInt(55);
		pm.makePersistent(tcD);
		
		pm.currentTransaction().commit();

		//delete one object
		pm.currentTransaction().begin();
		pm.deletePersistent(tcD);
		pm.currentTransaction().commit();
		pm.close();

	
		// test with closed TX
		assertGetIntFails(tc);
		assertGetIntFails(tcD);
		
		pm = pmf.getPersistenceManager();
		pm.currentTransaction().begin();
		
		// check access with wrong pm
		assertGetIntFails(tc);
		assertGetIntFails(tcD);

		//check deletion again
		try {
			pm.deletePersistent(tc);
			fail();
		} catch (JDOUserException e) {
			//good
		}
		pm.currentTransaction().commit();
		pm.close();
		
		
		//test query
		pm = pmf.getPersistenceManager();
		pm.currentTransaction().begin();

		Query q = pm.newQuery(TestClass.class, "_int >= 5");
		Collection<TestClass> c = (Collection<TestClass>) q.execute();
		Iterator<TestClass> it = c.iterator();
		assertEquals(5, it.next().getInt());
		assertFalse(it.hasNext());
		
		//close
		pm.currentTransaction().commit();
		pm.close();
		pmf.close();
	}
	
	private void assertGetIntFails(TestClass tc) {
		try {
			tc.getInt();
			fail();
		} catch (JDOUserException e) {
			//good
		}
	}
	
	@After
	public void afterTest() {
		if (pm != null && !pm.isClosed()) {
			if (pm.currentTransaction().isActive()) {
				pm.currentTransaction().rollback();
			}
			pm.close();
		}
		pm = null;
		if (pmf != null && !pmf.isClosed()) {
			pmf.close();
		}
		pmf = null;
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb(DB_NAME);
	}
}

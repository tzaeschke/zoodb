package org.zoodb.test.jdo;

import static org.junit.Assert.*;

import java.util.Collection;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.test.testutil.TestTools;

public class Test_171_QueryBug096 {

	@BeforeClass
	public static void setUp() {
        TestTools.removeDb();
		TestTools.createDb();
	}

	@Test
	public void test() {
		test(false);
	} 
	
	@Test
	public void testWithData() {
		test(true);
	} 
	
	private void test(boolean withData) {
		ZooJdoProperties props = TestTools.getProps();
		props.setZooAutoCreateSchema(true);
		PersistenceManager pm = TestTools.openPM(props);
		pm.currentTransaction().begin();

		if (withData) {
			TestClass book3 = new TestClass();
			book3.setInt(3);
			pm.makePersistent(book3);
			TestClass book20 = new TestClass();
			book20.setInt(20);
			pm.makePersistent(book20);
		}

		pm.currentTransaction().commit();

		pm.currentTransaction().begin();

		Query query = pm.newQuery(TestClass.class, "_int <= 7");

		query.setOrdering("_int ascending");
		// Calling setOrdering if there are no Books in the DB
		// results in a NullPointerException, however if there
		// are any Books in the DB, then it works.

		Object obj = query.execute();

		if (obj != null) {
			@SuppressWarnings("unchecked")
			Collection<TestClass> books = (Collection<TestClass>) obj;
			for (TestClass book : books) {
				assertTrue(0 < book.getInt());
			}
		}

		query.closeAll();

		pm.currentTransaction().commit();
		if (pm.currentTransaction().isActive()) {
			pm.currentTransaction().rollback();
		}
		pm.close();
		pm.getPersistenceManagerFactory().close();
	}
	
}

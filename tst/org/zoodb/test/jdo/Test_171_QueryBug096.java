package org.zoodb.test.jdo;

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
		ZooJdoProperties props = TestTools.getProps();
		props.setZooAutoCreateSchema(true);
		PersistenceManager pm = TestTools.openPM(props);
		pm.currentTransaction().begin();

		// pm.makePersistent(new Book(3));
		// pm.makePersistent(new Book(20));

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
			for (TestClass book : books)
				System.out.println("Found book with id: " + book.getInt() + ".");
		}

		query.closeAll();

		pm.currentTransaction().commit();
		if (pm.currentTransaction().isActive()) {
			pm.currentTransaction().rollback();
		}
		pm.close();
		pm.getPersistenceManagerFactory().close();

		System.out.println("Closed");
	}
	
}

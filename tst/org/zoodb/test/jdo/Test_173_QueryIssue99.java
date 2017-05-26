package org.zoodb.test.jdo;

import java.util.Collection;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.test.testutil.TestTools;

public class Test_173_QueryIssue99 {

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

		final String str = "abcdefghijklmnopqrstuvwxyz";
		pm.currentTransaction().begin();
		for (char c : str.toCharArray()) {
			TestClass i = new TestClass();
			i.setChar(c);
			pm.makePersistent(i);
		}
		pm.currentTransaction().commit();
		System.out.println("Items inserted.");

		pm.currentTransaction().begin();
		ZooJdoHelper.createIndex(pm, TestClass.class, "_char", true/*isUnique*/);
		pm.currentTransaction().commit();
		System.out.println("Index by Item.ch created.");

		pm.currentTransaction().begin();
		Query q = pm.newQuery(TestClass.class);
		q.declareParameters("Character ch1, Character ch2");
		q.setFilter("_char == ch1 || _char == ch2");
		Collection<TestClass> rslt = (Collection<TestClass>) q.execute('a', 'z');
		System.out.println(rslt.size() + " items found.");
		q.closeAll();
		pm.currentTransaction().commit();

		pm.close();
		pm.getPersistenceManagerFactory().close();
		System.out.println( "Successfully finished." );
	}

}

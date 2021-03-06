package org.zoodb.test.jdo;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;

import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.jdo.impl.QueryImpl;
import org.zoodb.test.testutil.TestTools;

public class Test_173_QueryIssue99_100_104 {

	@Before
	public void setUp() {
        TestTools.removeDb();
		TestTools.createDb();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testIssue99() {
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

		pm.currentTransaction().begin();
		ZooJdoHelper.createIndex(pm, TestClass.class, "_char", true/*isUnique*/);
		pm.currentTransaction().commit();

		pm.currentTransaction().begin();
		Query q = pm.newQuery(TestClass.class);
		q.declareParameters("Character ch1, Character ch2");
		q.setFilter("_char == ch1 || _char == ch2");
		Collection<TestClass> rslt = (Collection<TestClass>) q.execute('a', 'z');
		assertTrue(rslt.iterator().hasNext());
		q.closeAll();
		pm.currentTransaction().commit();

		TestTools.closePM();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testIssue100() {
		ZooJdoProperties props = TestTools.getProps();
		props.setZooAutoCreateSchema(true);
		PersistenceManager pm = TestTools.openPM(props);

		final String str = "abcdefghijklmnopqrstuvwxyz";
		pm.currentTransaction().begin();
		for (char c : str.toCharArray()) {
			TestClass i = new TestClass();
			i.setChar(c);
			i.setString( new String(new char[] { c, '9' }) );
			pm.makePersistent(i);
		}
		pm.currentTransaction().commit();

		pm.currentTransaction().begin();
		ZooJdoHelper.createIndex(pm, TestClass.class, "_string", true/*isUnique*/);
		pm.currentTransaction().commit();

		pm.currentTransaction().begin();
		Query q = pm.newQuery(TestClass.class);
		q.declareParameters("String ssp");
		q.setFilter("ssp != null && _string == ssp");
		Collection<TestClass> rslt = (Collection<TestClass>) q.execute("z9");
		q.closeAll();
		pm.currentTransaction().commit();

		pm.currentTransaction().begin();
		q = pm.newQuery(TestClass.class);
		q.setFilter(":ssp != null && _string == :ssp");
		try {
			rslt = (Collection<TestClass>) q.execute("z9");
			fail();
		} catch (JDOUserException e) {
			//good, V3 cannot parse this
		}

		boolean q4 = QueryImpl.ENFORCE_QUERY_V4;
		QueryImpl.ENFORCE_QUERY_V4 = true;
		q = pm.newQuery(TestClass.class);
		q.setFilter(":ssp != null && _string == :ssp");
		rslt = (Collection<TestClass>) q.execute("z9");
		assertTrue(rslt.iterator().hasNext());
		q.closeAll();
		pm.currentTransaction().commit();

		QueryImpl.ENFORCE_QUERY_V4 = q4;

		TestTools.closePM();
	}

	/**
	 * This fails/failed because 'a' was parsed as String, which could not
	 * be compared to Character.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testIssue104() {
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

		pm.currentTransaction().begin();
		ZooJdoHelper.createIndex(pm, TestClass.class, "_char", true/*isUnique*/);
		pm.currentTransaction().commit();

		pm.currentTransaction().begin();
		Query q = pm.newQuery(TestClass.class);
		q.setFilter("_char == 'a' || _char == 'z'");
		Collection<TestClass> rslt = (Collection<TestClass>) q.execute();
		assertTrue(rslt.iterator().hasNext());

		try {
			q.setFilter("_char == 'ab'");
			q.execute();
			fail();
		} catch (JDOUserException e) {
			//good
		}

		q.closeAll();
		pm.currentTransaction().commit();

		TestTools.closePM();
	}

	/**
	 * This ensures that 'a' can also be used to query String fields.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testIssue104b() {
		ZooJdoProperties props = TestTools.getProps();
		props.setZooAutoCreateSchema(true);
		PersistenceManager pm = TestTools.openPM(props);

		final String str = "abcdefghijklmnopqrstuvwxyz";
		pm.currentTransaction().begin();
		for (char c : str.toCharArray()) {
			TestClass i = new TestClass();
			i.setString(Character.toString(c));
			pm.makePersistent(i);
		}
		pm.currentTransaction().commit();

		//test without index
		pm.currentTransaction().begin();
		Query q = pm.newQuery(TestClass.class);
		q.setFilter("_string == 'a' || _string == 'z'");
		Collection<TestClass> rslt = (Collection<TestClass>) q.execute();
		assertTrue(rslt.iterator().hasNext());
		q.closeAll();
		pm.currentTransaction().commit();


		pm.currentTransaction().begin();
		ZooJdoHelper.createIndex(pm, TestClass.class, "_string", true/*isUnique*/);
		pm.currentTransaction().commit();

		//test with index
		pm.currentTransaction().begin();
		Query q2 = pm.newQuery(TestClass.class);
		q2.setFilter("_string == 'a' || _string == 'z'");
		Collection<TestClass> rslt2 = (Collection<TestClass>) q2.execute();
		assertTrue(rslt2.iterator().hasNext());
		q2.closeAll();
		pm.currentTransaction().commit();

		TestTools.closePM();
	}

	/**
	 * Check that comparing char fields to a String fails.
	 */
	@Test
	public void testIssue104c() {
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


		//Test without index
		pm.currentTransaction().begin();
		Query q = pm.newQuery(TestClass.class);
		try {
			q.setFilter("_char == 'ab'");
			q.execute();
			fail();
		} catch (JDOUserException e) {
			//good
		}
		q.closeAll();
		pm.currentTransaction().commit();


		pm.currentTransaction().begin();
		ZooJdoHelper.createIndex(pm, TestClass.class, "_char", true/*isUnique*/);
		pm.currentTransaction().commit();

		//test with index
		pm.currentTransaction().begin();
		Query q2 = pm.newQuery(TestClass.class);
		try {
			q2.setFilter("_char == 'ab'");
			q2.execute();
			fail();
		} catch (JDOUserException e) {
			//good
		}
		q2.closeAll();
		pm.currentTransaction().commit();

		TestTools.closePM();
	}

}

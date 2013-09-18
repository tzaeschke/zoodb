/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
 * 
 * This file is part of ZooDB.
 * 
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * See the README and COPYING files for further information. 
 */
package org.zoodb.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Collection;
import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.test.testutil.TestTools;

public class Test_077_QueryFieldIndexUpdates {

	@Before
	public void before() {
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
	}

	/**
	 * Tests that the index is correctly updated (entries removed and added) after object have
	 * changed.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testUpdatingObjectNonUniqueInt() {
		TestTools.defineIndex(TestClass.class, "_int" , false);

		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		for (int i = 0; i < 10; i++) {
			TestClass tc = new TestClass();
			tc.setInt(i);
			pm.makePersistent(tc);
		}
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		Extent<TestClass> ext = pm.getExtent(TestClass.class);
		for (TestClass t: ext) {
			if (t.getInt() < 5) {
				t.setInt(1);
			} else {
				t.setInt(11);
			}
		}
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//check < 5
		Query q = pm.newQuery(TestClass.class, "_int <= 10");
		Collection<TestClass> col = (Collection<TestClass>) q.execute();
		assertEquals(5, col.size());
		q.closeAll();
		
		//check > 10
		q = pm.newQuery(TestClass.class, "_int > 10");
		col = (Collection<TestClass>) q.execute();
		assertEquals(5, col.size());
		q.closeAll();
		
		//check sum
		q = pm.newQuery(TestClass.class);
		col = (Collection<TestClass>) q.execute();
		int n = 0;
		for (Iterator<?> i = col.iterator(); i.hasNext(); i.next()) {
			n++;
		}
		assertEquals(10, n);
		q.closeAll();
		
		pm.currentTransaction().rollback();
		TestTools.closePM(pm);
	}

	/**
	 * Tests that the index is correctly updated (entries removed and added) after object have
	 * changed.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testUpdatingObjectNonUniqueString() {
		TestTools.defineIndex(TestClass.class, "_string" , false);

		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		for (int i = 0; i < 10; i++) {
			TestClass tc = new TestClass();
			tc.setInt(i);
			tc.setString(""+i);
			pm.makePersistent(tc);
		}
		TestClass tc = new TestClass();
		tc.setInt(-1);
		tc.setString(null);
		pm.makePersistent(tc);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		Extent<TestClass> ext = pm.getExtent(TestClass.class);
		for (TestClass t: ext) {
			if (t.getInt() < 5) {
				t.setString(""+1);
			} else {
				t.setString(""+11);
			}
			if (t.getInt() == 4) {
				t.setString(null);
			}
		}
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//check < 5
		Query q = pm.newQuery(TestClass.class, "_string == '1'");
		Collection<TestClass> col = (Collection<TestClass>) q.execute();
		assertEquals(5, col.size());
		q.closeAll();
		
		//check > 10
		q = pm.newQuery(TestClass.class, "_string == '11'");
		col = (Collection<TestClass>) q.execute();
		assertEquals(5, col.size());
		q.closeAll();
		
		//check == null
//		q = pm.newQuery(TestClass.class, "_string == null");
//		col = (Collection<TestClass>) q.execute();
//		assertEquals(1, col.size());
//		q.closeAll();
		System.out.println("FIXME Test for _string==null");
		System.out.println("FIXME Test also for |_string!=null| == 10");
		
		//check sum
		q = pm.newQuery(TestClass.class);
		col = (Collection<TestClass>) q.execute();
		int n = 0;
		for (Iterator<?> i = col.iterator(); i.hasNext(); i.next()) {
			n++;
		}
		assertEquals(11, n);
		q.closeAll();
		
		pm.currentTransaction().rollback();
		TestTools.closePM(pm);
	}

	/**
	 * Tests that the index is correctly updated (entries removed and added) after object have
	 * been deleted.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testDeletingAndAdd() {
		TestTools.defineIndex(TestClass.class, "_string" , false);

		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		for (int i = 0; i < 10; i++) {
			TestClass tc = new TestClass();
			tc.setInt(i);
			tc.setString(""+i);
			pm.makePersistent(tc);
		}
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		Extent<TestClass> ext = pm.getExtent(TestClass.class);
		for (TestClass t: ext) {
			pm.deletePersistent(t);
		}
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//check total==0
		Query q = pm.newQuery(TestClass.class);
		Collection<TestClass> col = (Collection<TestClass>) q.execute();
		assertFalse(col.iterator().hasNext());
		q.closeAll();

		//Create again
		for (int i = 0; i < 10; i++) {
			TestClass tc = new TestClass();
			tc.setInt(i);
			tc.setString(""+i);
			pm.makePersistent(tc);
		}
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//check sum
		q = pm.newQuery(TestClass.class);
		col = (Collection<TestClass>) q.execute();
		int n = 0;
		for (Iterator<?> i = col.iterator(); i.hasNext(); i.next()) {
			n++;
		}
		assertEquals(10, n);
		q.closeAll();
		
		//check < 5
		q = pm.newQuery(TestClass.class, "_string == '1'");
		col = (Collection<TestClass>) q.execute();
		assertEquals(1, col.size());
		q.closeAll();
		
		//check > 10
		q = pm.newQuery(TestClass.class, "_string == '0'");
		col = (Collection<TestClass>) q.execute();
		assertEquals(1, col.size());
		q.closeAll();
		
		pm.currentTransaction().rollback();
		TestTools.closePM(pm);
	}

	/**
	 * Tests that the index is correctly updated (entries removed and added) after object have
	 * been deleted.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testDropInstAndAdd() {
		TestTools.defineIndex(TestClass.class, "_string" , false);

		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		for (int i = 0; i < 10; i++) {
			TestClass tc = new TestClass();
			tc.setInt(i);
			tc.setString(""+i);
			pm.makePersistent(tc);
		}
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		ZooSchema.locateClass(pm, TestClass.class).dropInstances();
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//check total==0
		Query q = pm.newQuery(TestClass.class);
		Collection<TestClass> col = (Collection<TestClass>) q.execute();
		assertFalse(col.iterator().hasNext());
		q.closeAll();

		//Create again
		for (int i = 0; i < 10; i++) {
			TestClass tc = new TestClass();
			tc.setInt(i);
			tc.setString(""+i);
			pm.makePersistent(tc);
		}
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//check sum
		q = pm.newQuery(TestClass.class);
		col = (Collection<TestClass>) q.execute();
		int n = 0;
		for (Iterator<?> i = col.iterator(); i.hasNext(); i.next()) {
			n++;
		}
		assertEquals(10, n);
		q.closeAll();
		
		//check < 5
		q = pm.newQuery(TestClass.class, "_string == '1'");
		col = (Collection<TestClass>) q.execute();
		assertEquals(1, col.size());
		q.closeAll();
		
		//check > 10
		q = pm.newQuery(TestClass.class, "_string == '0'");
		col = (Collection<TestClass>) q.execute();
		assertEquals(1, col.size());
		q.closeAll();
		
		pm.currentTransaction().rollback();
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

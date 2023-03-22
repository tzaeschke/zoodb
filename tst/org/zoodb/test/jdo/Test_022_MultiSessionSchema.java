/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zoodb.test.jdo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.JDOFatalUserException;
import javax.jdo.JDOHelper;
import javax.jdo.JDOOptimisticVerificationException;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.zoodb.internal.server.DiskAccessOneFile;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.schema.ZooClass;
import org.zoodb.schema.ZooSchema;
import org.zoodb.test.api.TestSuper;
import org.zoodb.test.testutil.TestTools;

public class Test_022_MultiSessionSchema {
	
	@Before
	public void setUp() {
		DiskAccessOneFile.allowReadConcurrency(true);
		TestTools.removeDb();
		TestTools.createDb();
	}
	
	@After
	public void tearDown() {
		DiskAccessOneFile.allowReadConcurrency(false);
		TestTools.removeDb();
	}
	
	@Test
	public void testCommitFailSchema() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf =
			JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm1 = pmf.getPersistenceManager();
		pm1.currentTransaction().begin();

		ZooSchema s1 = ZooJdoHelper.schema(pm1);

		//new TestSuper(1, 11, null);
		ZooClass t11 = s1.defineEmptyClass("Class11");
		s1.defineEmptyClass("Class22");
		ZooClass t13 = s1.defineEmptyClass("Class33");
		ZooClass t14 = s1.defineEmptyClass("Class44");
		s1.defineEmptyClass("Class55");

		//TODO test normal schema updates (new attr) and naming conflicts separately!
		//E.g. create class with name before cponciurring class is committed

		pm1.currentTransaction().commit();

		PersistenceManager pm2 = pmf.getPersistenceManager();

		pm1.currentTransaction().begin();

		pm2.currentTransaction().begin();
		ZooSchema s2 = ZooJdoHelper.schema(pm2);

		//concurrent modification
		ZooClass t21 = s2.getClass("Class11");
		ZooClass t22 = s2.getClass("Class22");
		ZooClass t23 = s2.getClass("Class33");
		ZooClass t24 = s2.getClass("Class44");
		ZooClass t25 = s2.getClass("Class55");

		//modified by both: t1, t3
		//modified by 1: t4
		//modified by 2: t5
		t21.addField("_id", Long.TYPE);
		t23.rename("Class23");
		t25.addField("_id", Long.class);

		t11.addField("_id2", Long.TYPE);
		t13.rename("Class13");
		t14.addField("_id", Long.class);

		pm2.currentTransaction().commit();

		try {
			pm1.currentTransaction().commit();
			fail();
		} catch (JDOFatalDataStoreException e) {
			//good!
			assertFalse(e instanceof JDOOptimisticVerificationException);
		}

		pm2.currentTransaction().begin();

		assertTrue(pm1.isClosed());

		assertEquals("_id", t21.getAllFields().get(0).getName());
		assertEquals(0, t22.getAllFields().size());
		assertEquals("Class23", t23.getName());
		assertEquals(0, t24.getAllFields().size());
		assertEquals("_id", t25.getAllFields().get(0).getName());


		pm2.currentTransaction().rollback();

		pm2.close();
		pmf.close();
	}

	@Test
	public void testCommitFailSchemaSmall() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf =
			JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm1 = pmf.getPersistenceManager();
		pm1.currentTransaction().begin();

		PersistenceManager pm2 = pmf.getPersistenceManager();
		pm2.currentTransaction().begin();
		ZooSchema s2 = ZooJdoHelper.schema(pm2);

		//session 1
		ZooSchema s1 = ZooJdoHelper.schema(pm1);
		s1.defineEmptyClass("Class11");

		//concurrent modification
		try {
			s2.defineEmptyClass("Class11");
		} catch (RuntimeException e) {
			e.printStackTrace();
		}


		pm2.currentTransaction().commit();
		pm2.currentTransaction().begin();

		try {
			pm1.currentTransaction().commit();
			fail();
		} catch (JDOFatalDataStoreException e) {
			//good!
			assertFalse(e instanceof JDOOptimisticVerificationException);
		}

		assertTrue(pm1.isClosed());

		pm2.currentTransaction().rollback();

		pm2.close();
		pmf.close();
	}

	@Test
	public void testCommitFailWithDeleteSchema() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf =
			JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm1 = pmf.getPersistenceManager();
		pm1.currentTransaction().begin();

		ZooSchema s1 = ZooJdoHelper.schema(pm1);

		//new TestSuper(1, 11, null);
		ZooClass t11 = s1.defineEmptyClass("Class11");
		ZooClass t12 = s1.defineEmptyClass("Class22");
		ZooClass t13 = s1.defineEmptyClass("Class33");
		ZooClass t14 = s1.defineEmptyClass("Class44");
		s1.defineEmptyClass("Class55");

		//TODO test normal schema updates (new attr) and naming conflicts separately!
		//E.g. create class with name before cponciurring class is committed

		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();

		PersistenceManager pm2 = pmf.getPersistenceManager();
		pm2.currentTransaction().begin();
		ZooSchema s2 = ZooJdoHelper.schema(pm2);

		//concurrent modification
		ZooClass t21 = s2.getClass("Class11");
		ZooClass t22 = s2.getClass("Class22");
		ZooClass t23 = s2.getClass("Class33");
		//ZooClass t24 = s2.getClass("Class44");
		ZooClass t25 = s2.getClass("Class55");

		//deleted by both: t1
		//del/mod t2, t3
		//deleted by 1: t4
		//deleted by 2: t5
		t21.remove();
		t22.remove();
		t23.rename("Class23");
		t25.remove();

		t11.remove();
		t12.rename("Class12");
		t13.remove();
		t14.remove();

		pm2.currentTransaction().commit();
		pm2.currentTransaction().begin();

		try {
			pm1.currentTransaction().commit();
			fail();
		} catch (JDOFatalDataStoreException e) {
			//good!
			assertFalse(e instanceof JDOOptimisticVerificationException);
		}

		assertTrue(pm1.isClosed());

		assertNull(s2.getClass("Class33"));
		assertNotNull(s2.getClass("Class23"));

		assertNull(s2.getClass("Class12"));
		assertNull(s2.getClass("Class22"));

		pm2.currentTransaction().rollback();

		pm2.close();
		pmf.close();
	}

	@Test
	public void testSchemaDropInstances() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf =
			JDOHelper.getPersistenceManagerFactory(props);
		TestTools.defineSchema(TestSuper.class);
		TestTools.defineSchema(TestClass.class);
		PersistenceManager pm1 = pmf.getPersistenceManager();
		PersistenceManager pm2 = pmf.getPersistenceManager();
		pm1.currentTransaction().begin();
		pm2.currentTransaction().begin();

		TestSuper t11 = new TestSuper(1, 11, null); //clean
		TestSuper t12 = new TestSuper(2, 22, null); //dirty
		TestSuper t13 = new TestSuper(3, 33, null); //deleted
		TestSuper t14 = new TestSuper(4, 44, null);
		TestSuper t15 = new TestSuper(5, 55, null);

		pm1.makePersistent(t11);
		pm1.makePersistent(t12);
		pm1.makePersistent(t13);
		pm1.makePersistent(t14);
		pm1.makePersistent(t15);

		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();

		//concurrent modification
		TestClass t21 = new TestClass();
		TestClass t22 = new TestClass();
		TestClass t23 = new TestClass();
		pm2.makePersistent(t21);
		pm2.makePersistent(t22);
		pm2.makePersistent(t23);

		//clean: t1
		//deleted t2
		//update: t3
		//delete class of other tx
		pm2.deletePersistent(t22);
		t23.setInt(23);
		ZooJdoHelper.schema(pm2).getClass(TestSuper.class).dropInstances();

		pm1.deletePersistent(t12);
		t13.setId(13);
		ZooJdoHelper.schema(pm1).getClass(TestClass.class).dropInstances();

		pm2.currentTransaction().commit();
		pm2.currentTransaction().begin();

		try {
			pm1.checkConsistency();
		} catch (JDOFatalDataStoreException e) {
			//good
		}

		try {
			pm1.currentTransaction().commit();
			fail();
		} catch (JDOFatalUserException e) {
			//dropInstances is a schema operation!
		}

		assertTrue(pm1.isClosed());

		assertTrue(JDOHelper.isDeleted(t22));

		pm2.currentTransaction().rollback();

		pm2.close();
		pmf.close();
	}

	@Test
	public void testSchemaDropClass() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf =
			JDOHelper.getPersistenceManagerFactory(props);
		TestTools.defineSchema(TestSuper.class);
		TestTools.defineSchema(TestClass.class);
		PersistenceManager pm1 = pmf.getPersistenceManager();
		PersistenceManager pm2 = pmf.getPersistenceManager();
		pm1.currentTransaction().begin();
		pm2.currentTransaction().begin();

		TestSuper t11 = new TestSuper(1, 11, null); //clean
		TestSuper t12 = new TestSuper(2, 22, null); //dirty
		TestSuper t13 = new TestSuper(3, 33, null); //deleted
		TestSuper t14 = new TestSuper(4, 44, null);
		TestSuper t15 = new TestSuper(5, 55, null);

		pm1.makePersistent(t11);
		pm1.makePersistent(t12);
		pm1.makePersistent(t13);
		pm1.makePersistent(t14);
		pm1.makePersistent(t15);

		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();

		//concurrent modification
		TestClass t21 = new TestClass();
		TestClass t22 = new TestClass();
		TestClass t23 = new TestClass();
		pm2.makePersistent(t21);
		pm2.makePersistent(t22);
		pm2.makePersistent(t23);

		//clean: t1
		//deleted t2
		//update: t3
		//delete class of other tx
		pm2.deletePersistent(t22);
		t23.setInt(23);
		ZooJdoHelper.schema(pm2).getClass(TestSuper.class).remove();

		pm1.deletePersistent(t12);
		t13.setId(13);
		ZooJdoHelper.schema(pm1).getClass(TestClass.class).remove();

		pm2.currentTransaction().commit();
		pm2.currentTransaction().begin();

		try {
			pm1.checkConsistency();
		} catch (JDOFatalDataStoreException e) {
			//good
		}

		try {
			pm1.currentTransaction().commit();
			fail();
		} catch (JDOFatalUserException e) {
			//good --> schema change
		}
		assertTrue(pm1.isClosed());

		//assert FALSE, because the class TestClass is still there, only TestSuper was removed!
		assertFalse(JDOHelper.isDeleted(t21));
		assertFalse(JDOHelper.isDeleted(t23));

		pm2.currentTransaction().rollback();

		pm2.close();
		pmf.close();
	}

	@Test
	public void testSchemaAttrIndexUpdates() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf =
			JDOHelper.getPersistenceManagerFactory(props);
		TestTools.defineSchema(TestSuper.class);
		TestTools.defineSchema(TestClass.class);
		PersistenceManager pm1 = pmf.getPersistenceManager();
		PersistenceManager pm2 = pmf.getPersistenceManager();
		pm1.currentTransaction().begin();
		pm2.currentTransaction().begin();

		TestSuper t11 = new TestSuper(1, 11, null);
		TestSuper t12 = new TestSuper(2, 22, null);
		TestSuper t13 = new TestSuper(3, 33, null);

		pm1.makePersistent(t11);
		pm1.makePersistent(t12);
		pm1.makePersistent(t13);

		ZooJdoHelper.schema(pm1).getClass(TestSuper.class).getField("_id").createIndex(false);

		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();


		ZooJdoHelper.schema(pm2).getClass(TestSuper.class).getField("_id").createIndex(false);

		try {
			pm2.currentTransaction().commit();
			fail();
		} catch (JDOOptimisticVerificationException e) {
			//good!
		}

		pm2.currentTransaction().rollback();
		pm2.currentTransaction().begin();

		//try again
		ZooJdoHelper.schema(pm2).getClass(TestSuper.class).getField("_time").createIndex(false);
		pm2.currentTransaction().commit();
		pm2.currentTransaction().begin();

		assertTrue(ZooJdoHelper.schema(pm1).getClass(TestSuper.class).getField("_id").hasIndex());
		//Well, pm1 simply does not know yet that this is indexed...
		//assertTrue(ZooJdoHelper.schema(pm1).getClass(TestSuper.class).getField("_time").hasIndex());
		assertTrue(ZooJdoHelper.schema(pm2).getClass(TestSuper.class).getField("_id").hasIndex());
		assertTrue(ZooJdoHelper.schema(pm2).getClass(TestSuper.class).getField("_time").hasIndex());

		pm1.currentTransaction().rollback();
		pm2.currentTransaction().rollback();

		pm1.close();
		pm2.close();
		pmf.close();
	}

	@Test
	public void testSchemaAttrIndexUpdatesUnique() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf =
			JDOHelper.getPersistenceManagerFactory(props);
		TestTools.defineSchema(TestSuper.class);
		TestTools.defineSchema(TestClass.class);
		PersistenceManager pm1 = pmf.getPersistenceManager();
		PersistenceManager pm2 = pmf.getPersistenceManager();
		pm1.currentTransaction().begin();
		pm2.currentTransaction().begin();

		TestSuper t11 = new TestSuper(1, 11, null);
		TestSuper t12 = new TestSuper(2, 22, null);
		TestSuper t13 = new TestSuper(3, 33, null);

		pm1.makePersistent(t11);
		pm1.makePersistent(t12);
		pm1.makePersistent(t13);

		ZooJdoHelper.schema(pm1).getClass(TestSuper.class).getField("_id").createIndex(true);

		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();


		//concurrent modification
		TestSuper t21 = new TestSuper(1, 11, null);
		pm2.makePersistent(t21);


		try {
			pm2.currentTransaction().commit();
			fail();
		} catch (JDOUserException e) {
			//good!
			assertTrue(e.getMessage().contains("Unique index clash"));
		}

		//try again
		pm2.currentTransaction().begin();
		t21.setId(21);
		pm2.makePersistent(t21);

		pm2.currentTransaction().commit();
		pm2.currentTransaction().begin();

		ZooJdoHelper.schema(pm2).getClass(TestSuper.class).getField("_time").createIndex(true);

		try {
			pm2.currentTransaction().commit();
			fail();
		} catch (JDOUserException e) {
			//good!
			assertTrue(e.getMessage().contains("Duplicate entry"));
		}
		pm2.currentTransaction().begin();

		//okay fix it in session 1
		t11.setTime(1234);
		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();

		//try again
		ZooJdoHelper.schema(pm2).getClass(TestSuper.class).getField("_time").createIndex(true);
		pm2.currentTransaction().commit();
		pm2.currentTransaction().begin();

		assertTrue(ZooJdoHelper.schema(pm1).getClass(TestSuper.class).getField("_id").hasIndex());
		//Well, pm1 simply does not know yet that this is indexed...
		//assertTrue(ZooJdoHelper.schema(pm1).getClass(TestSuper.class).getField("_time").hasIndex());
		assertTrue(ZooJdoHelper.schema(pm2).getClass(TestSuper.class).getField("_id").hasIndex());
		assertTrue(ZooJdoHelper.schema(pm2).getClass(TestSuper.class).getField("_time").hasIndex());

		pm1.currentTransaction().rollback();
		pm2.currentTransaction().rollback();

		pm1.close();
		pm2.close();
		pmf.close();
	}

	@Test
	public void testSchemaAttrIndexUpdatesUniqueBug5() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf =
			JDOHelper.getPersistenceManagerFactory(props);
		TestTools.defineSchema(TestSuper.class);
		PersistenceManager pm1 = pmf.getPersistenceManager();
		pm1.currentTransaction().begin();

		TestSuper t11 = new TestSuper(1, 11, null);
		TestSuper t12 = new TestSuper(2, 22, null);
		TestSuper t13 = new TestSuper(3, 33, null);

		pm1.makePersistent(t11);
		pm1.makePersistent(t12);
		pm1.makePersistent(t13);

		ZooJdoHelper.schema(pm1).getClass(TestSuper.class).getField("_id").createIndex(true);

		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();


		//concurrent modification
		TestSuper t21 = new TestSuper(1, 11, null);
		pm1.makePersistent(t21);


		try {
			pm1.currentTransaction().commit();
			fail();
		} catch (JDOUserException e) {
			//good!
			assertTrue(e.getMessage().contains("Unique index clash"));
		}

		//try again
		pm1.currentTransaction().begin();
		t21.setId(21);
		pm1.makePersistent(t21);

		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();

		ZooJdoHelper.schema(pm1).getClass(TestSuper.class).getField("_time").createIndex(true);

		try {
			pm1.currentTransaction().commit();
			fail();
		} catch (JDOUserException e) {
			//good!
			assertTrue(e.getMessage().contains("Duplicate entry"));
		}

		//okay fix it in session 1
		pm1.currentTransaction().begin();
		t11.setTime(1234);

		///////////
		// The following commit failed because of 'duplicate entry'even though the index should
		// not be in the database at this point (it got rejected in the previous commit).
		///////////
		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();

		//try again
		ZooJdoHelper.schema(pm1).getClass(TestSuper.class).getField("_time").createIndex(true);
		pm1.currentTransaction().commit();
		pm1.currentTransaction().begin();

		assertTrue(ZooJdoHelper.schema(pm1).getClass(TestSuper.class).getField("_id").hasIndex());
		assertTrue(ZooJdoHelper.schema(pm1).getClass(TestSuper.class).getField("_time").hasIndex());

		pm1.currentTransaction().rollback();

		pm1.close();
		pmf.close();
	}

	@Test
	public void testSchemaAutocreationHangs_Issue_138() {
		ZooJdoProperties props = TestTools.getProps();
		props.setMultiThreaded(true);
		//props.setZooAutoCreateSchema(false);
		PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pmMain = pmf.getPersistenceManager();
		pmMain.currentTransaction().begin();
		//ZooJdoHelper.schema(pmMain).addClass(TestClass.class);
		pmMain.currentTransaction().commit();

		PersistenceManager pmWorker = pmf.getPersistenceManager();

		Runnable r = new Runnable() {
			@Override
			public void run() {
				pmWorker.currentTransaction().begin();
				//pmWorker.makePersistent(new Apple());
				pmWorker.makePersistent(new TestClass());
				pmWorker.currentTransaction().commit();
				pmMain.currentTransaction().begin();
				System.out.print("begin last commit in thread... "); // is reached
				pmMain.currentTransaction().commit();
				System.out.println("done!"); // problem: is not reached
			}
		};
		Thread t = new Thread(r, "Some external thread");
		t.start();

	}
}

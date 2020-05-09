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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.jdo.Constants;
import javax.jdo.Extent;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.test.api.TestSuper;
import org.zoodb.test.testutil.TestTools;
import org.zoodb.tools.ZooConfig;

/**
 * This test performs create, update, query, extent and delete operations in parallel with
 * multiple threads on a single PersistenceManager. 
 * 
 * @author ztilmann
 *
 */
public class Test_025_SingleSessionConcurrency {
	
	private final int N = 10000;
	private final int COMMIT_INTERVAL = 250;
	private final int T = 8;
	
	private final static ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
	
	@BeforeClass
	public static void beforeClass() {
		ZooConfig.setFileManager(ZooConfig.FILE_MGR_IN_MEMORY);
	}

	@AfterClass
	public static void afterClass() {
		ZooConfig.setDefaults();
	}

	@Before
	public void setUp() {
		TestTools.removeDb();
		TestTools.createDb();

		TestTools.defineSchema(TestSuper.class);
		TestTools.defineIndex(TestSuper.class, "_id", false);
	}
	
	@After
	public void tearDown() {
		TestTools.closePM();
		try {
			TestTools.removeDb();
		} catch (IllegalStateException e) {
		    errors.add(e);
		}
		checkErrors();
	}

	private void checkErrors() {
	    Throwable t;
        if (!errors.isEmpty()) {
            RuntimeException e = new RuntimeException("errors: " + errors.size(), errors.peek());
            while ((t = errors.poll()) != null) {
                e.addSuppressed(t);
            }
			throw e;
		}
	}

	private abstract static class Worker extends Thread {
		final PersistenceManager pm;
		final int N;
		final int COMMIT_INTERVAL;
		int n = 0;
		final int ID;

		private Worker(int id, int n, int commitInterval, PersistenceManager pm) {
			this.ID = id;
			this.pm = pm;
			this.N = n;
			this.COMMIT_INTERVAL = commitInterval;
		}

		@Override
		public void run() {
			try {
				runWorker();
			} catch (Throwable t) {
				Test_025_SingleSessionConcurrency.errors.add(t);
				t.printStackTrace();
			}
		}
		
		abstract void runWorker();
		
	}


    private static class Reader extends Worker {

        private Reader(int id, int n, PersistenceManager pm) {
            super(id, n, -1, pm);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void runWorker() {
            Extent<TestSuper> ext = pm.getExtent(TestSuper.class);
            for (TestSuper t: ext) {
                assertTrue(t.getData()[0] >= 0 && t.getData()[0] < N);
                TestSuper t2 = (TestSuper) pm.getObjectById( JDOHelper.getObjectId(t) );
                assertEquals(t.getId(), t2.getId());
                if (t.getId() == ID && t.getTime() < N/2) {
                    n++;
                }
            }
            Collection<TestSuper> col = 
                    (Collection<TestSuper>) pm.newQuery(
                            TestSuper.class, "_id == " + ID + " && _time >= " + (N/2)).execute();
            for (TestSuper t: col) {
                assertEquals(t.getId(), ID);
                assertTrue(t.getData()[0] >= 0 && t.getData()[0] < N);
                TestSuper t2 = (TestSuper) pm.getObjectById( JDOHelper.getObjectId(t) );
                assertEquals(t.getId(), t2.getId());
                n++;
            }
        }
    }


    private static class QueryWorker extends Worker {
        private final Query query;

        private QueryWorker(int id, int n, PersistenceManager pm, Query query) {
            super(id, n, -1, pm);
            this.query = query;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void runWorker() {
            //query.setRange(0, N * 1231);
            Collection<TestSuper> col = (Collection<TestSuper>) query.execute(ID, 0, N/2);
            checkQuery(col);
            
            //query.setRange(0, N/4);
            col = (Collection<TestSuper>) query.execute(ID, N/2, N*3/4);//, N);
            checkQuery(col);

            //query.setRange(N/4, N/2);
            col = (Collection<TestSuper>) query.execute(ID, N*3/4, N);
            checkQuery(col);
        }
        
        private void checkQuery(Collection<TestSuper> col) {
            for (TestSuper t: col) {
                assertEquals(t.getId(), ID);
                assertTrue(t.getData()[0] >= 0 && t.getData()[0] < N);
                TestSuper t2 = (TestSuper) pm.getObjectById( JDOHelper.getObjectId(t) );
                assertEquals(t.getId(), t2.getId());
                n++;
            }
        }
    }


	private static class Writer extends Worker {

		private final ArrayList<Object> oids = new ArrayList<>();
		
		private Writer(int id, int n, int commitInterval, PersistenceManager pm) {
			super(id, n, commitInterval, pm);
		}

		@Override
		public void runWorker() {
			for (int i = 0; i < N; i++) {
				TestSuper o = new TestSuper(i, ID, new long[]{i});
				pm.makePersistent(o);
				oids.add(pm.getObjectId(o));
				n++;
			}
		}
	}

	private static class Deleter extends Worker {

		private Deleter(int id, int n, int commitInterval, PersistenceManager pm) {
			super(id, n, commitInterval, pm);
		}

		@SuppressWarnings("unchecked")
		@Override
		public void runWorker() {
			Query q = pm.newQuery(TestSuper.class, "_id==" + ID);
			Collection<TestSuper> col = (Collection<TestSuper>)q.execute();
			Iterator<TestSuper> iter = col.iterator();
			while (iter.hasNext()) {
				pm.deletePersistent(iter.next());
				n++;
				if (n % COMMIT_INTERVAL == 0) {
					//start a new query, just for fun...
					col = (Collection<TestSuper>) q.execute();
					iter = col.iterator(); 
				}
			}
		}
	}

	private static class Updater extends Worker {

		private static final int DELTA = 100;
		private final ArrayList<Object> oids = new ArrayList<Object>();
		
		private Updater(int id, int n, int commitInterval, ArrayList<Object> oids, 
				PersistenceManager pm) {
			super(id, n, commitInterval, pm);
			this.oids.addAll(oids);
		}

		@Override
		public void runWorker() {
			while (n < oids.size()) {
				TestSuper t = (TestSuper) pm.getObjectById(oids.get(n));
				n++;
				t.setId(t.getId()+DELTA);
			}
		}
	}

    /**
     * Test concurrent read. 
     * @throws InterruptedException when interrupted.
     */
    @Test
    public void testParallelRead() throws InterruptedException {
        PersistenceManager pm = TestTools.openPM();
        pm.setMultithreaded(true);
        pm.currentTransaction().begin();
        
        //write
        Writer w = new Writer(0, N, COMMIT_INTERVAL, pm);
        w.start();
        w.join();
        
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();
        
        //read
        ArrayList<Reader> readers = new ArrayList<>();
        for (int i = 0; i < T; i++) {
            readers.add(new Reader(0, N, pm));
        }

        for (Reader reader: readers) {
            reader.start();
        }

        for (Reader reader: readers) {
            reader.join();
        }
        checkErrors();
        for (Reader reader: readers) {
            assertEquals("id=" + reader.ID, N, reader.n);
        }
        
        pm.currentTransaction().commit();
        TestTools.closePM();
    }

    /**
     * Test concurrent read. 
     * @throws InterruptedException when interrupted.
     */
    @Test
    public void testParallelQuery() throws InterruptedException {
        PersistenceManager pm = TestTools.openPM();
        pm.setMultithreaded(true);
        pm.currentTransaction().begin();
        
        //write
        Writer w = new Writer(0, N, COMMIT_INTERVAL, pm);
        w.start();
        w.join();
        
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();
        Query query = pm.newQuery(TestSuper.class, "_id == :id && _time >= :min && _time < :max");
        
        //read
        ArrayList<QueryWorker> readers = new ArrayList<>();
        for (int i = 0; i < T; i++) {
            readers.add(new QueryWorker(0, N, pm, query));
        }

        for (QueryWorker reader: readers) {
            reader.start();
        }

        for (QueryWorker reader: readers) {
            reader.join();
        }
        checkErrors();
        for (QueryWorker reader: readers) {
            assertEquals("id=" + reader.ID, N, reader.n);
        }
        
        pm.currentTransaction().commit();
        TestTools.closePM();
    }

	/**
	 * Test concurrent write. 
     * @throws InterruptedException when interrupted.
	 */
	@Test
	public void testParallelWrite() throws InterruptedException {
		PersistenceManager pm = TestTools.openPM();
		pm.setMultithreaded(true);
		pm.currentTransaction().begin();

		//write
		ArrayList<Writer> writers = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			writers.add(new Writer(0, N, COMMIT_INTERVAL, pm));
		}
		for (Writer w: writers) {
			w.start();
		}
		for (Writer w: writers) {
			w.join();
			assertEquals(N, w.n);
		}
		
		//read
		Reader r = new Reader(0, N, pm);
		r.start();
		r.join();
		assertEquals(N * T, r.n);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}

	/**
	 * Test concurrent write. 
     * @throws InterruptedException when interrupted.
	 */
	@Test
	public void testParallelReadWrite() throws InterruptedException {
		PersistenceManager pm = TestTools.openPM();
		pm.setMultithreaded(true);
		pm.currentTransaction().begin();

		//read and write
		ArrayList<Thread> workers = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			workers.add(new Reader(i, N, pm));
			workers.add(new Writer(i, N, COMMIT_INTERVAL, pm));
		}
		for (Thread w: workers) {
			w.start();
		}
		for (Thread w: workers) {
			w.join();
			if (w instanceof Writer) {
				assertEquals(N, ((Writer)w).n);
			}
		}
		
		//read only
		ArrayList<Reader> readers = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			readers.add(new Reader(i, N, pm));
		}

		for (Reader reader: readers) {
			reader.start();
		}

		for (Reader reader: readers) {
			reader.join();
			assertEquals(N, reader.n);
		}
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}

	/**
	 * Updates object in parallel (each object by one thread only).
     * @throws InterruptedException when interrupted.
	 */
	@Test
	public void testParallelUpdater() throws InterruptedException {
		PersistenceManager pm = TestTools.openPM();
		pm.setMultithreaded(true);
		pm.currentTransaction().begin();

		//read and write
		ArrayList<Writer> writers = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			writers.add(new Writer(i, N, COMMIT_INTERVAL, pm));
		}
		for (Writer w: writers) {
			w.start();
		}
		for (Writer w: writers) {
			w.join();
			assertEquals(N, w.n);
		}
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//update objects in parallel (no object touched twice)
		ArrayList<Updater> updaters = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			updaters.add(new Updater(i, N, COMMIT_INTERVAL, writers.get(i).oids, pm));
		}
		for (Updater w: updaters) {
			w.start();
		}
		for (Updater w: updaters) {
			w.join();
			assertEquals(N, w.n);
		}
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//read only
		ArrayList<Reader> readers = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			readers.add(new Reader(i+Updater.DELTA, N, pm));
		}

		for (Reader reader: readers) {
			reader.start();
		}

		for (Reader reader: readers) {
			reader.join();
			assertEquals("ID=" + reader.ID, N, reader.n);
		}
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}

	/**
	 * Update the same objects concurrently from several threads. 
     * @throws InterruptedException when interrupted.
	 */
	@Test
	public void testConcurrentUpdater() throws InterruptedException {
		PersistenceManager pm = TestTools.openPM();
		pm.setMultithreaded(true);
		pm.currentTransaction().begin();

		//read and write
		Writer w = new Writer(0, N, COMMIT_INTERVAL, pm);
		w.start();
		w.join();
				
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//update objects concurrently (objects updated concurrently)
		ArrayList<Updater> updaters = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			updaters.add(new Updater(0, N, COMMIT_INTERVAL, w.oids, pm));
		}
		for (Updater u: updaters) {
			u.start();
		}
		for (Updater u: updaters) {
			u.join();
			assertEquals(N, w.n);
		}
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//read only
		Reader r = new Reader(T*Updater.DELTA, N, pm);
		r.start();
		r.join();
		assertTrue("N="+N + "  r.n="+ r.n, N >= r.n);
		assertTrue(r.n > 0);
		
		for (Object oid: w.oids) {
			TestSuper t = (TestSuper) pm.getObjectById(oid);
			//All objects should show at least one increment
			assertTrue(t.getId() >= Updater.DELTA);
		}

		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}

	@Test
	public void testParallelDeleter() throws InterruptedException {
		PersistenceManager pm = TestTools.openPM();
		pm.setMultithreaded(true);
		pm.currentTransaction().begin();

		//write
		ArrayList<Writer> writers = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			writers.add(new Writer(i, N, COMMIT_INTERVAL, pm));
		}
		for (Writer w: writers) {
			w.start();
		}
		for (Writer w: writers) {
			w.join();
			assertEquals(N, w.n);
		}
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//delete
		ArrayList<Deleter> workers = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			workers.add(new Deleter(i, N, COMMIT_INTERVAL, pm));
		}
		for (Deleter w: workers) {
			w.start();
		}
		for (Deleter w: workers) {
			w.join();
			assertEquals(N, w.n);
		}
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
	@Test
	public void testPmAPI() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		assertTrue(pm.getMultithreaded());
		
		pm.setMultithreaded(false);
		assertFalse(pm.getMultithreaded());
		
		pm.setMultithreaded(true);
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}
	
	@Test
	public void testPmfAPI() {
		PersistenceManager pm = TestTools.openPM();
		PersistenceManagerFactory pmf = pm.getPersistenceManagerFactory();
		pm.close();
		assertTrue(pmf.getMultithreaded());
		
		pm = pmf.getPersistenceManager();
		assertTrue(pm.getMultithreaded());
		pm.close();
		
		pmf.setMultithreaded(false);
		assertFalse(pmf.getMultithreaded());
		
		pm = pmf.getPersistenceManager();
		assertFalse(pm.getMultithreaded());
		pm.close();
		
		pmf.setMultithreaded(true);
		assertTrue(pmf.getMultithreaded());

		pm = pmf.getPersistenceManager();
		assertTrue(pm.getMultithreaded());
		pm.close();

		pmf.close();
	}
	
	@Test
	public void testConfig() {
		ZooJdoProperties props = TestTools.getProps();
		props.setMultiThreaded(true);
		PersistenceManager pm = TestTools.openPM(props);
		pm.currentTransaction().begin();

		pm.setMultithreaded(true);
		assertTrue(pm.getMultithreaded());
		
		pm.setMultithreaded(false);
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
		
		
		
		props = TestTools.getProps();
		props.setMultiThreaded(false);
		assertFalse(Boolean.parseBoolean(props.getProperty(Constants.PROPERTY_MULTITHREADED)));
		pm = TestTools.openPM(props);
		pm.currentTransaction().begin();

		assertFalse(pm.getMultithreaded());
		
		pm.setMultithreaded(false);
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}
}

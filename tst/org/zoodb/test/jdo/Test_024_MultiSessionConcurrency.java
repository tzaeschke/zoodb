/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.test.jdo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.JDOHelper;
import javax.jdo.JDOOptimisticVerificationException;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.test.api.TestSuper;
import org.zoodb.test.testutil.TestTools;

public class Test_024_MultiSessionConcurrency {
	
	private final int N = 1000; //TODO 10000
	private final int COMMIT_INTERVAL = 250;
	private final int T = 4;
	
	private final static ArrayList<Throwable> errors = new ArrayList<>();

	@Before
	public void setUp() {
		TestTools.removeDb();
		TestTools.createDb();

		TestTools.defineSchema(TestSuper.class);
		TestTools.defineIndex(TestSuper.class, "_id", false);
	}
	
	@After
	public void tearDown() {
		TestTools.removeDb();
		if (!errors.isEmpty()) {
			RuntimeException e = new RuntimeException("errors: " + errors.size(), errors.get(0));
			for (Throwable t: errors) {
				e.addSuppressed(t);
			}
			errors.clear();
			throw e;
		}
	}
	
	
	
	private abstract static class Worker extends Thread {

		final PersistenceManager pm;
		final int N;
		final int COMMIT_INTERVAL;
		int n = 0;
		final int ID;

		private Worker(int id, int n, int commitInterval) {
			this.ID = id;
			this.pm = ZooJdoHelper.openDB(TestTools.getDbName());
			this.N = n;
			this.COMMIT_INTERVAL = commitInterval;
		}

		@Override
		public void run() {
			try {
				pm.currentTransaction().begin();
				runWorker();
				pm.currentTransaction().rollback();
			} catch (Throwable t) {
				Test_024_MultiSessionConcurrency.errors.add(t);
				t.printStackTrace();
			} finally {
				closePM(pm);
			}
		}
		
		abstract void runWorker();
	}


	private static class Reader extends Worker {

		private Reader(int id, int n) {
			super(id, n, -1);
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
				assertTrue(t.getId() == ID);
				assertTrue(t.getData()[0] >= 0 && t.getData()[0] < N);
				TestSuper t2 = (TestSuper) pm.getObjectById( JDOHelper.getObjectId(t) );
				assertEquals(t.getId(), t2.getId());
				n++;
			}
		}
	}


	private static class Writer extends Worker {

		private final ArrayList<Object> oids = new ArrayList<>();
		
		private Writer(int id, int n, int commitInterval) {
			super(id, n, commitInterval);
		}

		@Override
		public void runWorker() {
			for (int i = 0; i < N; i++) {
				TestSuper o = new TestSuper(i, ID, new long[]{i});
				pm.makePersistent(o);
				oids.add(pm.getObjectId(o));
				n++;
				if (n % COMMIT_INTERVAL == 0) {
					pm.currentTransaction().commit();
					pm.currentTransaction().begin();
				}
			}
			pm.currentTransaction().commit();
			pm.currentTransaction().begin();
		}
	}

	private static class Deleter extends Worker {

		private Deleter(int id, int n, int commitInterval) {
			super(id, n, commitInterval);
		}

		@SuppressWarnings("unchecked")
		@Override
		public void runWorker() {
			Extent<TestSuper> ext = pm.getExtent(TestSuper.class);
			Iterator<TestSuper> iter = ext.iterator();
			while (iter.hasNext() && n < N/2) {
				pm.deletePersistent(iter.next());
				n++;
				if (n % COMMIT_INTERVAL == 0) {
					pm.currentTransaction().commit();
					pm.currentTransaction().begin();
					ext = pm.getExtent(TestSuper.class);
					iter = ext.iterator();
				}
			}
			ext.closeAll();
			
			Collection<TestSuper> col = 
					(Collection<TestSuper>) pm.newQuery(TestSuper.class).execute();
			iter = col.iterator();
			while (iter.hasNext() && n < N) {
				pm.deletePersistent(iter.next());
				n++;
				if (n % COMMIT_INTERVAL == 0) {
					pm.currentTransaction().commit();
					pm.currentTransaction().begin();
					col = (Collection<TestSuper>) pm.newQuery(TestSuper.class).execute();
					iter = col.iterator(); 
				}
			}
			pm.currentTransaction().commit();
			pm.currentTransaction().begin();
		}
	}

	private static class Updater extends Worker {

		private static final int DELTA = 100;
		private final ArrayList<Object> oids = new ArrayList<Object>();
		
		private Updater(int id, int n, int commitInterval, ArrayList<Object> oids) {
			super(id, n, commitInterval);
			this.oids.addAll(oids);
		}

		@Override
		public void runWorker() {
			while (!oids.isEmpty()) {
				try { 
					TestSuper t = (TestSuper) pm.getObjectById(oids.get(0));
					oids.remove(0);
					n++;
					t.setId(t.getId()+DELTA);
					if (n % COMMIT_INTERVAL == 0) {
						pm.currentTransaction().commit();
						pm.currentTransaction().begin();
					}
				} catch (JDOOptimisticVerificationException e) {
					pm.currentTransaction().begin();
					n -= e.getNestedExceptions().length;
					for (Throwable t: e.getNestedExceptions()) {
						Object f = ((JDOOptimisticVerificationException)t).getFailedObject();
						oids.add(pm.getObjectId(f));
					}
				}
			}
			
			pm.currentTransaction().commit();
			pm.currentTransaction().begin();
		}
	}

	private static void closePM(PersistenceManager pm) {
		if (!pm.isClosed()) {
			if (pm.currentTransaction().isActive()) {
				pm.currentTransaction().rollback();
			}
			pm.close();
		}
	}

	/**
	 * Test concurrent read. 
	 * @throws InterruptedException 
	 */
	@Test
	public void testParallelRead() throws InterruptedException {
		//write
		Writer w = new Writer(0, N, COMMIT_INTERVAL);
		w.start();
		w.join();
		
		//read
		ArrayList<Reader> readers = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			readers.add(new Reader(0, N));
		}

		for (Reader reader: readers) {
			reader.start();
		}

		for (Reader reader: readers) {
			reader.join();
			assertEquals("id=" + reader.ID, N, reader.n);
		}
	}

	/**
	 * Test concurrent write. 
	 * @throws InterruptedException 
	 */
	@Test
	public void testParallelWrite() throws InterruptedException {
		//write
		ArrayList<Writer> writers = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			writers.add(new Writer(0, N, COMMIT_INTERVAL));
		}
		for (Writer w: writers) {
			w.start();
		}
		for (Writer w: writers) {
			w.join();
			assertEquals(N, w.n);
		}
		
		//read
		Reader r = new Reader(0, N);
		r.start();
		r.join();
		assertEquals(N * T, r.n);
	}

	/**
	 * Test concurrent write. 
	 * @throws InterruptedException 
	 */
	@Test
	public void testParallelReadWrite() throws InterruptedException {
		//read and write
		ArrayList<Thread> workers = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			workers.add(new Reader(i, N));
			workers.add(new Writer(i, N, COMMIT_INTERVAL));
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
			readers.add(new Reader(i, N));
		}

		for (Reader reader: readers) {
			reader.start();
		}

		for (Reader reader: readers) {
			reader.join();
			assertEquals(N, reader.n);
		}
	}

	@Test
	public void testParallelUpdater() throws InterruptedException {
		//read and write
		ArrayList<Writer> writers = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			writers.add(new Writer(i, N, COMMIT_INTERVAL));
		}
		for (Writer w: writers) {
			w.start();
		}
		for (Writer w: writers) {
			w.join();
			assertEquals(N, w.n);
		}
		
		//update objects in parallel (no object touched twice)
		ArrayList<Updater> updaters = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			updaters.add(new Updater(i, N, COMMIT_INTERVAL, writers.get(i).oids));
		}
		for (Updater w: updaters) {
			w.start();
		}
		for (Updater w: updaters) {
			w.join();
			assertEquals(N, w.n);
		}
		
		//read only
		ArrayList<Reader> readers = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			readers.add(new Reader(i+Updater.DELTA, N));
		}

		for (Reader reader: readers) {
			reader.start();
		}

		for (Reader reader: readers) {
			reader.join();
			assertEquals("ID=" + reader.ID, N, reader.n);
		}
	}

	@Test
	public void testConcurrentUpdater() throws InterruptedException {
		//read and write
		Writer w = new Writer(0, N, COMMIT_INTERVAL);
		w.start();
		w.join();
				
		//update objects concurrently (objects updated concurrently)
		ArrayList<Updater> updaters = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			updaters.add(new Updater(0, N, COMMIT_INTERVAL, w.oids));
		}
		for (Updater u: updaters) {
			u.start();
		}
		for (Updater u: updaters) {
			u.join();
			assertEquals(N, w.n);
		}
		
		//read only
		Reader r = new Reader(T*Updater.DELTA, N);
		r.start();
		r.join();
		assertEquals(N, r.n);
	}

	@Test
	public void testParallelDeleter() throws InterruptedException {
		//write
		ArrayList<Writer> writers = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			writers.add(new Writer(i, N, COMMIT_INTERVAL));
		}
		for (Writer w: writers) {
			w.start();
		}
		for (Writer w: writers) {
			w.join();
			assertEquals(N, w.n);
		}
		
		//delete
		ArrayList<Deleter> workers = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			workers.add(new Deleter(i, N, COMMIT_INTERVAL));
		}
		for (Deleter w: workers) {
			w.start();
		}
		for (Deleter w: workers) {
			w.join();
			assertEquals(N, w.n);
		}
	}
}

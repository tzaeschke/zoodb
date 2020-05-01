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
package org.zoodb.jdo.perf.large;

import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.zoodb.jdo.ZooJdoHelper;

/**
 * This example tests performance for storing a large amount of data.
 * 
 */
public class ExamplePerfLargeMain {

	private static final String DB_FILE = "examplePerfLarge.zdb";

	private PersistenceManager pm;
	
	public static void main(final String[] args) {
		new ExamplePerfLargeMain().run();
	}

	private void run() {
		ZooJdoHelper.removeDb(DB_FILE);

		System.out.println("> Inserting data ...");
		insertData();
		System.out.println("> Data insertion complete!");

		System.out.println("> Running queries ...");
		executeQueries();
		System.out.println("> Queries complete!");
	}

	private void insertData() {
		int nMax = 50_000_000;
		int nBatchSize = 1_000_000;
		PcData[] data = new PcData[nMax];
		for (int i = 0; i < nMax; i++) {
			int x = i % 10000;
			String str = "hhh"+x;
			PcData d = new PcData(str, str, "yyyyyyyyyy132313y", "xxx", i, i*0.5f);
			data[i] = d;
		}
		
		long t0 = System.currentTimeMillis();
		pm = ZooJdoHelper.openOrCreateDB(DB_FILE);
		
		int n = 0;
		while (n < nMax) {
			long t10 = System.currentTimeMillis();
			pm.currentTransaction().begin();
			for (int i2 = 0; i2 < nBatchSize; i2++) {
				pm.makePersistent(data[n]);
				n++;
			}
			pm.currentTransaction().commit();			
			long t11 = System.currentTimeMillis();
			System.out.println("Time insertion (" +nBatchSize+ "): " + (t11-t10) + "   n=" + n);
		}
		long t1 = System.currentTimeMillis();
		System.out.println("Time insertion: " + (t1-t0));
		
		t0 = System.currentTimeMillis();
		pm.currentTransaction().begin();
		ZooJdoHelper.createIndex(pm, PcData.class, "f1", false);
		pm.currentTransaction().commit();
		t1 = System.currentTimeMillis();
		System.out.println("Time index f1: " + (t1-t0));

		t0 = System.currentTimeMillis();
		pm.currentTransaction().begin();
		ZooJdoHelper.createIndex(pm, PcData.class, "s1", false);
		pm.currentTransaction().commit();
		t1 = System.currentTimeMillis();
		System.out.println("Time index s1: " + (t1-t0));
		
		pm.close();
		pm = null;
	}

	private void executeQueries() {
		pm = ZooJdoHelper.openDB(DB_FILE);
		
		pm.currentTransaction().begin();
		
		query();

		pm.currentTransaction().commit();
		pm.close();
		pm = null;
	}

	@SuppressWarnings("unchecked")
	private void query() {
		Query q = pm.newQuery(PcData.class, "s1.startsWith('hhh" + 3333 + "')");
		List<PcData> data = (List<PcData>)q.execute();
		for (PcData p : data) {
			System.out.println(">> - " + p.getS1());
		}
		System.out.println(">> Query for People instances returned results: " + data.size());
	}

}

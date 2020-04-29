/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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

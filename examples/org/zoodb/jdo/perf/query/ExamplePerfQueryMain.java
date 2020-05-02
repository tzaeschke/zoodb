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
package org.zoodb.jdo.perf.query;

import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.zoodb.jdo.ZooJdoHelper;

/**
 * This example executes a large amount of queries. 
 * 
 */
public class ExamplePerfQueryMain {

	private static final String DB_FILE = "examplePerfQuery.zdb";

	private PersistenceManager pm;
	
	public static void main(final String[] args) {
		new ExamplePerfQueryMain().run();
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
		pm = ZooJdoHelper.openOrCreateDB(DB_FILE);
		pm.currentTransaction().begin();

		for (int i = 0; i < 10000; i++) {
			Person p = new Person("Name" + i, 10+(i % 80));
			pm.makePersistent(p);
		}

		ZooJdoHelper.createIndex(pm, Person.class, "name", false);
		ZooJdoHelper.createIndex(pm, Person.class, "age", false);
		
		pm.currentTransaction().commit();
		pm.close();
		pm = null;
	}

	private void executeQueries() {
		pm = ZooJdoHelper.openDB(DB_FILE);
		
		pm.currentTransaction().begin();
		
		int nQuery = 100_000;
		for (int i = 0; i < 3; i++) {
			queryByAge(false, false, nQuery);
			queryByAge(false, true, nQuery);
			queryByAge(true, false, nQuery);
			System.out.println();
		}

		nQuery = 10_000;
		for (int i = 0; i < 3; i++) {
			queryByAgeRange(false, false, nQuery);
			queryByAgeRange(false, true, nQuery);
			queryByAgeRange(true, false, nQuery);
			System.out.println();
		}

		pm.currentTransaction().commit();
		pm.close();
		pm = null;
	}

	@SuppressWarnings("unchecked")
	private void queryByAge(boolean fixed, boolean preCompile, int nQuery) {
		Query q = null;
		long t1 = System.currentTimeMillis(); 
		if (preCompile) {
			q = pm.newQuery(Person.class);
			q.declareParameters("int anAge");
			q.setFilter("age == anAge");
		}
		int nFound = 0;
		for (int i = 0; i < nQuery; i++) {
			if (!preCompile && !fixed) {
				q = pm.newQuery(Person.class);
				q.declareParameters("int anAge");
				q.setFilter("age == anAge");

			}
			List<Person> persons;
			if (fixed) {
				q = pm.newQuery(Person.class);
				q.setFilter("age == " + (i % 50));
				persons = (List<Person>) q.execute();
			} else {
				persons = (List<Person>) q.execute(i % 50);
			}
			for (Person person : persons) {
				nFound++;
			}
		}
		long t2 = System.currentTimeMillis(); 
		System.out.println(">> Query for People instances returned results: " + nFound + "  dt=" + (t2-t1) 
				+ " preCompile=" + preCompile);
	}

	private void queryByAgeRange(boolean fixed, boolean preCompile, int nQuery) {
		Query q = null;
		long t1 = System.currentTimeMillis(); 
		if (preCompile) {
			q = pm.newQuery(Person.class);
			q.declareParameters("int anAge, int anAge2");
			q.setFilter("age >= anAge && age <= anAge2");
		}
		int nFound = 0;
		for (int i = 0; i < nQuery; i++) {
			if (!preCompile && !fixed) {
				q = pm.newQuery(Person.class);
				q.declareParameters("int anAge, int anAge2");
				q.setFilter("age >= anAge && age <= anAge2");

			}
			List<Person> persons;
			if (fixed) {
				q = pm.newQuery(Person.class);
				q.setFilter("age >= " + (i % 50) + " && age <= " + ((i % 50)+5));
				persons = (List<Person>) q.execute();
			} else {
				persons = (List<Person>) q.execute(i % 50, (i % 50)+5);
			}
			for (Person person : persons) {
				nFound++;
			}
		}
		long t2 = System.currentTimeMillis(); 
		System.out.println(">> Query for People range instances returned results: " + nFound + "  dt=" + (t2-t1) 
				+ " preCompile=" + preCompile);
	}

}

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
package org.zoodb.jdo.perf.query;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.jdo.Extent;
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
		System.out.println(">> Query for People instances returned results: " + nFound + "  dt=" + (t2-t1) 
				+ " preCompile=" + preCompile);
	}

}

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
package org.zoodb.test.zoo;

import org.junit.Test;
import org.zoodb.ZooSession;
import org.zoodb.tools.ZooHelper;

public class Test_000_SmokeTest {

	@Test
	public void main() {
		String dbName = "ExampleDB.zdb";
		createDB(dbName);
		populateDB(dbName);
		readDB(dbName);
	}


	/**
	 * Read data from a database.
	 * Extents are fast, but allow filtering only on the class.
	 * Queries are a bit more powerful than Extents.
	 *  
	 * @param dbName Database name.
	 */
	private void readDB(String dbName) {
		ZooSession s = ZooSession.open(dbName);
		s.begin();

//		//Extents are one way to get objects from a database:
//		System.out.println("Person extent: ");
//		Extent<Person> ext = pm.getExtent(Person.class);
//		for (Person p: ext) {
//			System.out.println("Person found: " + p.getName());
//		}
//		ext.closeAll();

		//Queries are more powerful:
		System.out.println("Queries: ");
//		Query query = pm.newQuery(Person.class, "name == 'Bart'");
//		Collection<Person> barts = (Collection<Person>) query.execute();
//		for (Person p: barts) {
//			System.out.println("Person found called 'Bart': " + p.getName());
//		}
//		query.closeAll();
//
//		//Once an object is loaded, normal method calls can be used to traverse the object graph.
//		Person bart = barts.iterator().next();
//		System.out.println(bart.getName() + " has " + bart.getFriends().size() + " friend(s):");
//		for (Person p: bart.getFriends()) {
//			System.out.println(p.getName() + " is a friend of " + bart.getName());
//		}

		s.commit();
		closeDB(s);
	}


	/**
	 * Populate a database.
	 * 
	 * ZooDB supports persistence by reachability. This means that if 'lisa' is stored in the
	 * database, 'bart' will also be stored because it is referenced from 'lisa'.
	 * The zooActivate(...) methods in {@code Person.addFriend()} ensure that 'bart' is flagged as modified
	 * when {@code addFriend()} is called, so in the second part an updated 'bart' and 'maggie'
	 * will be stored.
	 * 
	 * @param dbName Database name.
	 */
	private void populateDB(String dbName) {
		ZooSession s = ZooSession.open(dbName);
		s.begin();

		// create instances
		Person lisa = new Person("Lisa");
		//make Lisa persistent. 
		s.makePersistent(lisa);

		//add Bart to Lisa's friends
		Person bart = new Person("Bart");
		lisa.addFriend(bart);

		s.commit();
		s.begin();

		bart.addFriend(new Person("Maggie"));

		s.commit();
		closeDB(s);
	}


	/**
	 * Create a database.
	 * 
	 * @param dbName Name of the database to create.
	 */
	private void createDB(String dbName) {
		// remove database if it exists
		if (ZooHelper.dbExists(dbName)) {
			ZooHelper.removeDb(dbName);
		}

		// create database
		// By default, all database files will be created in %USER_HOME%/zoodb
		ZooHelper.createDb(dbName);
	}


	/**
	 * Close the database connection.
	 * 
	 * @param s The current ZooSession.
	 */
	private void closeDB(ZooSession s) {
		s.close();
	}

}

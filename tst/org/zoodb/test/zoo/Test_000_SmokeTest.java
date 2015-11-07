/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
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
	 * @param pm The current PersistenceManager.
	 */
	private void closeDB(ZooSession s) {
		s.close();
	}

}

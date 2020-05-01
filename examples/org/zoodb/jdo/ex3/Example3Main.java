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
package org.zoodb.jdo.ex3;

import java.util.Collection;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.ex2.Course;
import org.zoodb.jdo.ex2.Person;
import org.zoodb.jdo.ex2.School;
import org.zoodb.jdo.ex2.Student;
import org.zoodb.jdo.ex2.Teacher;

/**
 * This example that shows how to access object outside of transactions with
 * non-transactional read. This is, for example, useful in GUI
 * applications where a transaction should be closed most of the time,
 * while the GUI should still be able to read data from the database.
 * 
 * Note that in a setting with multiple concurrent persistence managers,
 * this may result in inconsistencies because
 * any values that were read outside of a transaction may be outdated by
 * the time when the next transaction commits. 
 * 
 */
public class Example3Main {

	private static final String DB_FILE = "example3.zdb";

	private PersistenceManager pm;
	
	public static void main(final String[] args) {
		new Example3Main().run();
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

		School school = new School("Good School");

		Teacher t1 = new Teacher("Alan Turing", 1100.00);
		Teacher t2 = new Teacher("Albert Einstein", 4056);
		Teacher t3 = new Teacher("Adam Ries", 7.36);
		Teacher t4 = new Teacher("Archimedes", 34.32);

		school.addTeacher(t1);
		school.addTeacher(t2);
		school.addTeacher(t3);
		school.addTeacher(t4);

		Student s1 = new Student("Bart", 1);
		Student s2 = new Student("Lisa", 2);
		school.addStudent(s1);
		school.addStudent(s2);

		Course cAlg = new Course(t1, "Algebra");
		Course cSpo = new Course(t2, "Physics");
		Course cEng = new Course(t4, "Engineering");
		school.addCourse(cAlg);
		school.addCourse(cSpo);
		school.addCourse(cEng);
		cAlg.addStudents(s2);
		cSpo.addStudents(s1, s2);

		//We only store the root object, school. Everything else is stored transitively, because
		//it is referenced from 'school'.
		pm.makePersistent(school);

		//define a non-unique index on the name of all Teachers and Students
		ZooJdoHelper.createIndex(pm, Person.class, "name", false);
		
		pm.currentTransaction().commit();
		pm.close();
		pm = null;
	}

	private void executeQueries() {
		pm = ZooJdoHelper.openDB(DB_FILE);
		
		//set non-transactional read to 'true', but don't begin() yet.
		pm.currentTransaction().setNontransactionalRead(true);
		
		//use queries and navigation outside of a transaction:
		queryForPeople();
		queryForCoursesByTeacher("Albert Einstein");

		// now, if we want to change something, we can open a trancaction
		pm.currentTransaction().begin();
		// change something here
		pm.currentTransaction().commit();
		pm.close();
		pm = null;
	}

	private void queryForPeople() {
		Extent<Person> persons = pm.getExtent(Person.class);
		System.out.println(">> Query for People instances returned results:");
		for (Person person : persons) {
			System.out.println(">> - " + person.getName());
		}
	}

	/**
	 * Example for a path query on Course.teacher.name == myName.
	 * @param name
	 */
	@SuppressWarnings("unchecked")
	private void queryForCoursesByTeacher(String name) {
		System.out.println(">> Query for courses by teacher " + name + " returned:");
		//using reference in query
		Query q = pm.newQuery(Course.class, "teacher.name == '" + name + "'");
		Collection<Course> courses = (Collection<Course>)q.execute(); 
		for (Course c : courses) {
			System.out.println(">> - " + c.getName() + " by " + c.getTeacher().getName());
		}
	}	

}

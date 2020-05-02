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
package org.zoodb.jdo.ex2;

import java.util.Collection;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.zoodb.jdo.ZooJdoHelper;

/**
 * Slightly advanced example that uses inheritance, indexing and navigation.
 * The queries show path queries and the use of Java methods in queries.
 * Note that JDOQL / ZooDB support only some methods that are available in 
 * normal Java classes.
 */
public class Example2Main {

	private static final String DB_FILE = "example2.zdb";

	private PersistenceManager pm;
	
	public static void main(final String[] args) {
		new Example2Main().run();
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
		pm.currentTransaction().begin();

		// All people (show names).
		queryForPeople();

		// All courses (titles) with given teacher.
		queryForCoursesByTeacher("Albert Einstein");

		// The course with the most students.
		queryForCoursesWithMaxStudentCount();

		
		// All courses with 2 students.
		queryForCoursesWithXStudents(2);
		
		// Find all courses whose teacher have a first name and a last name
		queryForCoursesWithTeachersWithFirstAndLastName();
		
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
	 * @param name Teacher name
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
	
	/**
	 * This example demonstrates that using 'navigation'may often be a good alternative
	 * for queries, especially if the term in question (collection.size()) cannot be indexed yet.
	 */
	private void queryForCoursesWithMaxStudentCount() {
		//TODO `MAX` in queries doesn't work yet, so we use navigation
		Extent<Course> courses = pm.getExtent(Course.class);
		Course maxCourse = null;
		int maxStudents = -1;
		for (Course c: courses) {
			if (c.getStudents().size() > maxStudents) {
				maxStudents = c.getStudents().size();
				maxCourse = c;
			}
		}
		if (maxCourse != null) {
			System.out.println(">> Query for course with most students returned: " +
					maxCourse.getName());
		} else {
			System.out.println(">> Query for course with most students returned no courses.");
		}
	}
	
	/**
	 * This example demonstrates how many Java methods of Java SE classes can
	 * be used in queries. Not that not all methods can be used.
	 */
	@SuppressWarnings("unchecked")
	private void queryForCoursesWithXStudents(int studentCount) {
		System.out.println(">> Query for courses with " + studentCount + " students:");
		//using Java method in query
		Query q = pm.newQuery(Course.class, "students.size() == " + studentCount + "");
		Collection<Course> courses = (Collection<Course>)q.execute(); 
		for (Course c : courses) {
			System.out.println(">> - " + c.getName() + " has size: " + c.getStudents().size());
		}
	}

	
	/**
	 * This example combines a path query with a Java method call on the String class.
	 */
	@SuppressWarnings("unchecked")
	private void queryForCoursesWithTeachersWithFirstAndLastName() {
		System.out.println(">> Query for courses whose teacher have a frist and last name:");
		//using Java method in query
		Query q = pm.newQuery(Course.class, "teacher.name.indexOf(' ') >= 1");
		Collection<Course> courses = (Collection<Course>)q.execute(); 
		for (Course c : courses) {
			System.out.println(">> - " + c.getName() + 
					" has teacher: " + c.getTeacher().getName());
		}
	}
}

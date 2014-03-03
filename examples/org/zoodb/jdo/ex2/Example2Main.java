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
package org.zoodb.jdo.ex2;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;

import org.zoodb.jdo.ZooJdoHelper;

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
		school.addCourse(cAlg);
		school.addCourse(cSpo);
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

		// All courses (titles) with given teacher and student.
		queryForCourseByTeacherWithStudent("Albert Einstein", "Bart");

		// All teachers (names) with given student.
		queryForTeacherThatHaveAGivenStudentViaCourse("Lisa");

		// The course with the most students.
		queryForCoursesWithMaxStudentCount();

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

	private void queryForCoursesByTeacher(final String name) {
		System.out.println(">> Query for courses by teacher " + name + " returned:");
		//TODO references in query don't work yet, so we use navigation
		Extent<Course> courses = pm.getExtent(Course.class);
		for (Course c : courses) {
			if (c.getTeacher().getName().equals(name)) {
				System.out.println(">> - " + c.getName());
			}
		}
	}

	private void queryForCourseByTeacherWithStudent(final String teacher, final String student) {
//		//SODA
//		Query query = storageManager.query();
//		query.constrain(Course.class);
//		query.descend("teacher").descend("name").constrain(teacher);
//		query.descend("students").descend("name").constrain(student);
//		List<Course> result = query.execute();
//
//		System.out.println(">> Query for course by " + teacher +
//				" and " + student + " returned " +
//				result.size() + " results (should 1):");
//		for (Course c : result) {
//			System.out.println(">> - " + c.getName());
//		}
	}

	private void queryForTeacherThatHaveAGivenStudentViaCourse(final String student) {
//		//QBE:
//		//School s = storageManager.queryBE(new School(null)).get(0);
//		//Set<Teacher> result = s.getTeachers();
//
//		//SODA
//		Query query = storageManager.query();
//		query.constrain(Course.class);
//		query.descend("students").descend("name").constrain(student);
//		Query teacherQuery = query.descend("teacher");
//		List<Teacher> result = storageManager.result(teacherQuery);
//
//		System.out.println(">> Query teachers that teach " + student + " returned " +
//				result.size() + " results (should 2):");
//		for (Teacher v : result) {
//			System.out.println(">> - " + v.getName());
//		}
	}

	private void queryForCoursesWithMaxStudentCount() {
		//TODO collections.size() in query don't work yet, so we use navigation
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

}

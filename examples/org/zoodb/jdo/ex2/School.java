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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.zoodb.api.impl.ZooPCImpl;

public class School extends ZooPCImpl {

	private String name;
	private Set<Teacher> teachers = new HashSet<>();
	private Set<Student> students = new HashSet<>();
	private Set<Course> courses = new HashSet<>();
	
	
	public School(String name) {
		this.name = name;
	}
	
	public String getName() {
		zooActivateRead();
		return name;
	}
	
	public void addTeacher(Teacher t) {
		zooActivateWrite();
		teachers.add(t);
	}
	
	public void addStudent(Student s) {
		zooActivateWrite();
		students.add(s);
	}
	
	public void addCourse(Course c) {
		zooActivateWrite();
		courses.add(c);
	}

	public Set<Teacher> getTeachers() {
		zooActivateRead();
		return Collections.unmodifiableSet(teachers);
	}
}

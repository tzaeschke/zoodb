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

import java.util.HashSet;
import java.util.Set;

import org.zoodb.api.impl.ZooPC;

public class Course extends ZooPC {

	private String name;

	private Set<Student> students = new HashSet<Student>();
	private Teacher teacher;
	
	@SuppressWarnings("unused")
	private Course() {
		//just for ZooDB
	}

	public Course(Teacher teacher, String name) {
		this.name = name;
		this.teacher = teacher;
	}
	
	@Override
	public String toString() {
		zooActivateRead();
		return "Course: " + name;
	}

	public void addStudents(Student ... students) {
		zooActivateWrite();
		for (Student s: students) {
			this.students.add(s);
		}
	}

	public String getName() {
		zooActivateRead();
		return name;
	}

	public Set<Student> getStudents() {
		zooActivateRead();
		return students;
	}
	
	public Teacher getTeacher() {
		zooActivateRead();
		return teacher;
	}
}

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

public class Teacher extends Person {

	private double salary;
	
	private Teacher() {
		super(null);
		//just for ZooDB
	}
	
	public Teacher(String name, double salary) {
		super(name);
		this.salary = salary;
	}
	
	public double getSalary() {
		zooActivateRead();
		return salary;
	}
}

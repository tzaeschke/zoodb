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

import org.zoodb.api.impl.ZooPC;

public class Person extends ZooPC {

	private String name;
	private int age;
	
	private Person() {
		//for JDO
	}
	
	public Person(String name, int age) {
		this.name = name;
		this.age = age;
	}
	
	public String getName() {
		zooActivateRead();
		return name;
	}

	public int getAge() {
		zooActivateRead();
		return age;
	}
	
	@Override
	public String toString() {
		zooActivateRead();
		return name;
	}
	
}

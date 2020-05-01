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

import org.zoodb.api.impl.ZooPC;

public abstract class Person extends ZooPC {

	private String name;
	
	public Person(String name) {
		this.name = name;
	}
	
	public String getName() {
		zooActivateRead();
		return name;
	}
	
	@Override
	public String toString() {
		zooActivateRead();
		return name;
	}
	
}

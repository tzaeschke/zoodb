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
package org.zoodb.test.jdo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("unused")
public class TestQueryClass extends TestClass {

	private List<Integer> listI;
	private List<TestClass> listTC;
	private List<Object> listObj;
	private Set<Object> set;
	private Map<Object, Object> map;
	private Collection<Object> coll;
	private TestQueryClass ref;
	
	public void init() {
		zooActivateWrite();
		listI = new ArrayList<>();
		listTC = new ArrayList<>();
		listObj = new ArrayList<>();
		set = new HashSet<>();
		map = new HashMap<>();
		coll = new ArrayList<>();
	}
	
	public void addInt(int i) {
		zooActivateWrite();
		listI.add(i);
	}
	
	public void addTC(TestClass tc) {
		zooActivateWrite();
		listTC.add(tc);
	}
	
	public void addObj(Object obj) {
		zooActivateWrite();
		listObj.add(obj);
	}
	
	public void addToSet(Object obj) {
		zooActivateWrite();
		set.add(obj);
	}
	
	public void addToMap(Object key, Object val) {
		zooActivateWrite();
		map.put(key, val);
	}
	
	public void addToColl(Object obj) {
		zooActivateWrite();
		coll.add(obj);
	}
	
	public void setRef(TestQueryClass r) {
		zooActivateWrite();
		ref = r;
	}
}

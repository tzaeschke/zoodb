/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.test.jdo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.zoodb.api.impl.ZooPC;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

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

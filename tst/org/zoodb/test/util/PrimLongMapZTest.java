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
package org.zoodb.test.util;

import static org.junit.Assert.*;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;
import org.zoodb.internal.util.PrimLongMap;
import org.zoodb.internal.util.PrimLongMapZ;

/**
 * Test harness for PrimLongTreeMap.
 *
 * @author  Tilmann Zaeschke
 */
public final class PrimLongMapZTest extends PrimLongMapTest {

	@Override
	protected PrimLongMap<String> createMap() {
		return new PrimLongMapZ	<String>();
	}
 
	@Test 
	public void bruteForce() {
		int N = 100000;
		int N1 = 10;
		PrimLongMapZ<Long> map = new PrimLongMapZ<>();
		//Write 1st half
		for (int i = 0; i < N; i++) {
			map.put(i, Long.valueOf(-i));
		}
		assertEquals(N, map.size());

		//check get
		for (int i = 0; i < N1; i++) {
			assertEquals(Long.valueOf(-i), map.get(i));
			assertTrue(map.containsKey(i));
		}
		List<PrimLongMap.PrimLongEntry<Long>> entryList = map.entrySet().stream().sorted(
				(o1, o2)-> Long.compare(o1.getKey(), o2.getKey())).
                collect(Collectors.toList());
		for (int i = 0; i < N1; i++) {
			assertEquals(Long.valueOf(-i), entryList.get(i).getValue());
		}
		List<Long> keyList = map.keySet().stream().sorted(
				(o1, o2)-> Long.compare(o1, o2)).collect(Collectors.toList());
		for (int i = 0; i < N1; i++) {
			assertEquals(Long.valueOf(i), keyList.get(i));
		}
		List<Long> valueList = map.values().stream().sorted(
				(o1, o2)-> Long.compare(o1, o2)).collect(Collectors.toList());
		for (int i = 0; i < N1; i++) {
			assertEquals(Long.valueOf(i-N+1), valueList.get(i));
		}
		

		//Write 2nd half
		for (int i = N1; i < N; i++) {
			map.put(i, Long.valueOf(-i));
		}
		assertEquals(N, map.size());

		//check get
		for (int i = 0; i < N; i++) {
			assertEquals(Long.valueOf(-i), map.get(i));
			assertTrue(map.containsKey(i));
		}
		entryList = map.entrySet().stream().sorted(
				(o1, o2)-> Long.compare(o1.getKey(), o2.getKey())).
                collect(Collectors.toList());
		for (int i = 0; i < N; i++) {
			assertEquals(Long.valueOf(-i), entryList.get(i).getValue());
		}
		keyList = map.keySet().stream().sorted(
				(o1, o2)-> Long.compare(o1, o2)).collect(Collectors.toList());
		for (int i = 0; i < N; i++) {
			assertEquals(Long.valueOf(i), keyList.get(i));
		}
		valueList = map.values().stream().sorted(
				(o1, o2)-> Long.compare(o1, o2)).collect(Collectors.toList());
		for (int i = 0; i < N; i++) {
			assertEquals(Long.valueOf(i-N+1), valueList.get(i));
		}
		

		//Write again
		for (int i = 0; i < N; i++) {
			map.put(i, Long.valueOf(i));
		}
		assertEquals(N, map.size());
		
		//check get
		for (int i = 0; i < N; i++) {
			assertEquals(Long.valueOf(i), map.get(i));
			assertTrue(map.containsKey(i));
		}
		entryList = map.entrySet().stream().sorted(
				(o1, o2)-> Long.compare(o1.getKey(), o2.getKey())).
                collect(Collectors.toList());
		for (int i = 0; i < N; i++) {
			assertEquals(Long.valueOf(i), entryList.get(i).getValue());
		}
		
		//remove some
		for (int i = 0; i < N; i+=3) {
			map.remove(i);
		}
		assertEquals(N*2/3, map.size());
		
		//check get
		int pos = 0;
		for (int i = 0; i < N; i++) {
			if (pos % 3 != 0) {
				assertEquals(Long.valueOf(i), map.get(pos++));
				assertTrue(map.containsKey(pos - 1));
			}
		}
		entryList = map.entrySet().stream().sorted(
				(o1, o2)-> Long.compare(o1.getKey(), o2.getKey())).
                collect(Collectors.toList());
		pos = 0;
		for (int i = 0; i < N; i++) {
			if (i % 3 != 0) {
				assertEquals(Long.valueOf(i), entryList.get(pos++).getValue());
			}
		}
		pos = 0;
		keyList = map.keySet().stream().sorted(
				(o1, o2)-> Long.compare(o1, o2)).collect(Collectors.toList());
		for (int i = 1; i < N; i++) {
			if (pos % 3 != 0) {
				assertEquals(Long.valueOf(i), keyList.get(pos++));
			}
		}
		pos = 0;
		valueList = map.values().stream().sorted(
				(o1, o2)-> Long.compare(o1, o2)).collect(Collectors.toList());
		for (int i = 0; i < N; i++) {
			if (pos % 3 != 0) {
				assertEquals(Long.valueOf(i), valueList.get(pos++));
			}
		}

		//clear
		map.clear();
		assertEquals(0, map.size());
		assertFalse(map.values().iterator().hasNext());
		assertFalse(map.entrySet().iterator().hasNext());
		assertFalse(map.keySet().iterator().hasNext());
	}
	
}

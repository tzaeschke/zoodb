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
		for (long i = 0; i < N; i++) {
			map.put(i, -i);
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
				Long::compare).collect(Collectors.toList());
		for (int i = 0; i < N1; i++) {
			assertEquals(Long.valueOf(i), keyList.get(i));
		}
		List<Long> valueList = map.values().stream().sorted(
				Long::compare).collect(Collectors.toList());
		for (int i = 0; i < N1; i++) {
			assertEquals(Long.valueOf(i-N+1), valueList.get(i));
		}
		

		//Write 2nd half
		for (long i = N1; i < N; i++) {
			map.put(i, -i);
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
				Long::compare).collect(Collectors.toList());
		for (int i = 0; i < N; i++) {
			assertEquals(Long.valueOf(i), keyList.get(i));
		}
		valueList = map.values().stream().sorted(
				Long::compare).collect(Collectors.toList());
		for (int i = 0; i < N; i++) {
			assertEquals(Long.valueOf(i-N+1), valueList.get(i));
		}
		

		//Write again
		for (long i = 0; i < N; i++) {
			map.put(i, i);
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
		for (int i = 0; i < N; i++) {
			if (i % 3 != 0) {
				assertEquals(Long.valueOf(i), map.get(i));
				assertTrue(map.containsKey(i));
			} else {
                assertNull(map.get(i));
                assertFalse(map.containsKey(i));
			}
		}
		entryList = map.entrySet().stream().sorted(
				(o1, o2)-> Long.compare(o1.getKey(), o2.getKey())).
                collect(Collectors.toList());
        int pos = 0;
		for (int i = 0; i < N; i++) {
			if (i % 3 != 0) {
				assertEquals(Long.valueOf(i), entryList.get(pos++).getValue());
			}
		}
		pos = 0;
		keyList = map.keySet().stream().sorted(
				Long::compare).collect(Collectors.toList());
		for (int i = 1; i < N; i++) {
			if (i % 3 != 0) {
				assertEquals(Long.valueOf(i), keyList.get(pos++));
			}
		}
		pos = 0;
		valueList = map.values().stream().sorted(
				Long::compare).collect(Collectors.toList());
		for (int i = 0; i < N; i++) {
			if (i % 3 != 0) {
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

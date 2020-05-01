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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.zoodb.internal.util.ClosableIteratorWrapper;

/**
 * Test harness for ClosableIteratorWrapper.
 *
 * @author  Tilmann Zaeschke
 */
public final class ClosableIteratorWrapperTest {

	private List<Integer> list;
	private ClosableIteratorWrapper<Integer> it;

	/**
	 * Run before each test.
	 * The setUp method tests the put method.
	 */
	@Before
	public void before() {
		//create the lists
		list = new LinkedList<Integer>();
		list.add(21);
		list.add(22);
		list.add(23);
		it = new ClosableIteratorWrapper<Integer>(list.iterator(), null, true);
	}

	/**
	 * Test the size.
	 */
	@Test
	public void testSize() {
		int n = 0;
		while (it.hasNext()) {
			assertNotNull(it.next());
			n++;
		}
		assertEquals("Check size", 3, n);
	}

	/**
	 * Test the iterator method iterates over the correct collection.
	 */
	@Test
	public void testIterator() {
		assertEquals(21, (int)it.next());
		assertEquals(22, (int)it.next());
		assertEquals(23, (int)it.next());
		assertFalse("Check the number of remaining elements", it.hasNext());
	}

	/**
	 * Test remove.
	 */
	@Test
	public void testRemove() {
		for (int i = 0; i < 3; i++) {
			it.next();
			try { 
				it.remove();
				fail();
			} catch (UnsupportedOperationException e) {
				//good
			}
		}
		assertFalse("Check size", it.hasNext());
	}
}

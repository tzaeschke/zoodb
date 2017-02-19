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

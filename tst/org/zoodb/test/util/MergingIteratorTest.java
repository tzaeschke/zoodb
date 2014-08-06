/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.zoodb.internal.util.CloseableIterator;
import org.zoodb.internal.util.MergingIterator;

/**
 * Test harness for MerginIterator.
 *
 * @author  Tilmann Zaeschke
 */
public final class MergingIteratorTest {

	private List<Integer> list1;
	private List<Integer> list2;
	private List<Integer> list3;
	private MergingIterator<Integer> it;

	/**
	 * Run before each test.
	 * The setUp method tests the put method.
	 */
	@Before
	public void before() {
		//create the lists
		list1 = new LinkedList<Integer>();
		list1.add(11);
		list1.add(12);
		list2 = new LinkedList<Integer>();
		list2.add(21);
		list2.add(22);
		list2.add(23);
		list3 = new LinkedList<Integer>();
		list3.add(31);
		list3.add(32);
		list3.add(33);
		list3.add(34);
		it = new MergingIterator<Integer>();
		it.add(toCI(list1.iterator()));
		it.add(toCI(list2.iterator()));
		it.add(toCI(list3.iterator()));
	}

	private <T> CloseableIterator<T> toCI(final Iterator<T> it) {
		return new CloseableIterator<T>() {
			@Override
			public boolean hasNext() {
				return it.hasNext();
			}

			@Override
			public T next() {
				return it.next();
			}

			@Override
			public void remove() {
				it.remove();
			}

			@Override
			public void close() {
				// nothing to do
			}
		};
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
		assertEquals("Check size", 9, n);
	}

	/**
	 * Test the iterator method iterates over the correct collection.
	 */
	@Test
	public void testIterator() {
		assertEquals(11, (int)it.next());
		assertEquals(12, (int)it.next());
		assertEquals(21, (int)it.next());
		assertEquals(22, (int)it.next());
		assertEquals(23, (int)it.next());
		assertEquals(31, (int)it.next());
		assertEquals(32, (int)it.next());
		assertEquals(33, (int)it.next());
		assertEquals(34, (int)it.next());
		assertFalse("Check the number of remaining elements", it.hasNext());
	}

	/**
	 * Test remove.
	 */
	@Test
	public void testRemove() {
//		for (int i = 0; i < 9; i++) {
//			it.next();
//			it.remove();
//		}
//		assertFalse("Check size", it.hasNext());
		for (int i = 0; i < 9; i++) {
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

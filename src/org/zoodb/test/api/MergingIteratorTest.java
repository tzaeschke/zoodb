/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.test.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.internal.util.CloseableIterator;
import org.zoodb.jdo.internal.util.MergingIterator;

/**
 * Test harness for MerginIterator.
 *
 * @author  Tilmann Zaeschke
 */
public final class MergingIteratorTest {

	private List<Integer> _list1;
	private List<Integer> _list2;
	private List<Integer> _list3;
	private MergingIterator<Integer> _it;

	/**
	 * Run before each test.
	 * The setUp method tests the put method.
	 * @throws StoreException
	 */
	@Before
	public void before() {
		//create the lists
		_list1 = new LinkedList<Integer>();
		_list1.add(11);
		_list1.add(12);
		_list2 = new LinkedList<Integer>();
		_list2.add(21);
		_list2.add(22);
		_list2.add(23);
		_list3 = new LinkedList<Integer>();
		_list3.add(31);
		_list3.add(32);
		_list3.add(33);
		_list3.add(34);
		_it = new MergingIterator<Integer>();
		_it.add(toCI(_list1.iterator()));
		_it.add(toCI(_list2.iterator()));
		_it.add(toCI(_list3.iterator()));
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
		while (_it.hasNext()) {
			assertNotNull(_it.next());
			n++;
		}
		assertEquals("Check size", 9, n);
	}

	/**
	 * Test the iterator method iterates over the correct collection.
	 */
	@Test
	public void testIterator() {
		assertEquals(11, (int)_it.next());
		assertEquals(12, (int)_it.next());
		assertEquals(21, (int)_it.next());
		assertEquals(22, (int)_it.next());
		assertEquals(23, (int)_it.next());
		assertEquals(31, (int)_it.next());
		assertEquals(32, (int)_it.next());
		assertEquals(33, (int)_it.next());
		assertEquals(34, (int)_it.next());
		assertFalse("Check the number of remaining elements", _it.hasNext());
	}

	/**
	 * Test remove.
	 */
	@Test
	public void testRemove() {
		for (int i = 0; i < 9; i++) {
			_it.next();
			_it.remove();
		}
		assertFalse("Check size", _it.hasNext());
	}
}

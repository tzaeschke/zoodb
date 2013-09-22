/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;
import org.zoodb.jdo.internal.util.CloseableIterator;
import org.zoodb.jdo.internal.util.OrderedMergeIterator;

/**
 * Test harness for OrderedMergeIterator.
 *
 * @author  Tilmann Zaeschke
 */
public final class OrderedMergeIteratorTest {

	private List<LLEntry> list1;
	private List<LLEntry> list2;
	private List<LLEntry> list3;
	private OrderedMergeIterator it;

	/**
	 * Run before each test.
	 * The setUp method tests the put method.
	 * @throws StoreException
	 */
	@SuppressWarnings("unchecked")
	@Before
	public void before() {
		//create the lists
		list1 = new LinkedList<LLEntry>();
		add(list1,11);
		add(list1,12);
		list2 = new LinkedList<LLEntry>();
		add(list2,01);
		add(list2,12);
		add(list2,23);
		list3 = new LinkedList<LLEntry>();
		add(list3,10);
		add(list3,11);
		add(list3,12);
		add(list3,13);
		it = new OrderedMergeIterator(new CloseableIterator[]{
				toCI(list1.iterator()),
				toCI(list2.iterator()),
				toCI(list3.iterator())});
	}

	private void add(List<LLEntry> list, int v) {
		LLEntry e = new LLEntry(v, 1234);
		list.add(e);	
	}
	
	private CloseableIterator<LLEntry> toCI(final Iterator<LLEntry> it) {
		return new CloseableIterator<LLEntry>() {
			@Override
			public boolean hasNext() {
				return it.hasNext();
			}

			@Override
			public LLEntry next() {
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

            @Override
            public void refresh() {
                throw new UnsupportedOperationException();
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
		assertEquals(01, (int)it.next().getKey());
		assertEquals(10, (int)it.next().getKey());
		assertEquals(11, (int)it.next().getKey());
		assertEquals(11, (int)it.next().getKey());
		assertEquals(12, (int)it.next().getKey());
		assertEquals(12, (int)it.next().getKey());
		assertEquals(12, (int)it.next().getKey());
		assertEquals(13, (int)it.next().getKey());
		assertEquals(23, (int)it.next().getKey());
		assertFalse("Check the number of remaining elements", it.hasNext());
	}

	/**
	 * Test remove.
	 */
	@Test
	public void testRemove() {
		for (int i = 0; i < 9; i++) {
			it.next();
			it.remove();
		}
		assertFalse("Check size", it.hasNext());
	}

	/**
	 * Test empty.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testEmpty() {
		list1.clear();
		list2.clear();
		list3.clear();
		
		it = new OrderedMergeIterator(new CloseableIterator[]{
				toCI(list1.iterator()),
				toCI(list2.iterator()),
				toCI(list3.iterator())});

		assertFalse(it.hasNext());
	}
}

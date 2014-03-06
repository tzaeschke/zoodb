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
import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.util.CloseableIterator;
import org.zoodb.internal.util.OrderedMergeIterator;

/**
 * Test harness for OrderedMergeIterator.
 *
 * @author  Tilmann Zaeschke
 */
public final class OrderedMergeIteratorTest {

	private List<LongLongIndex.LLEntry> list1;
	private List<LongLongIndex.LLEntry> list2;
	private List<LongLongIndex.LLEntry> list3;
	private OrderedMergeIterator it;

	/**
	 * Run before each test.
	 * The setUp method tests the put method.
	 */
	@SuppressWarnings("unchecked")
	@Before
	public void before() {
		//create the lists
		list1 = new LinkedList<LongLongIndex.LLEntry>();
		add(list1,11);
		add(list1,12);
		list2 = new LinkedList<LongLongIndex.LLEntry>();
		add(list2,01);
		add(list2,12);
		add(list2,23);
		list3 = new LinkedList<LongLongIndex.LLEntry>();
		add(list3,10);
		add(list3,11);
		add(list3,12);
		add(list3,13);
		it = new OrderedMergeIterator(new CloseableIterator[]{
				toCI(list1.iterator()),
				toCI(list2.iterator()),
				toCI(list3.iterator())});
	}

	private void add(List<LongLongIndex.LLEntry> list, int v) {
		LongLongIndex.LLEntry e = new LongLongIndex.LLEntry(v, 1234);
		list.add(e);	
	}
	
	private CloseableIterator<LongLongIndex.LLEntry> toCI(final Iterator<LongLongIndex.LLEntry> it) {
		return new CloseableIterator<LongLongIndex.LLEntry>() {
			@Override
			public boolean hasNext() {
				return it.hasNext();
			}

			@Override
			public LongLongIndex.LLEntry next() {
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
			try { 
				it.remove();
				fail();
			} catch (UnsupportedOperationException e) {
				System.err.println("FIXME: OrderedMergeIterator.remove() not implemented.");
				//TODO For now we expect this to fail, but later?
			}
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

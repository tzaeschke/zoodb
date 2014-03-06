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
package org.zoodb.internal.server.index;

import java.util.NoSuchElementException;

import org.zoodb.internal.util.CloseableIterator;

public interface LongLongIndex {

	public static class LLEntry {
		private final long key;
		private final long value;
		public LLEntry(long k, long v) {
			key = k;
			value = v;
		}
		public long getKey() {
			return key;
		}
		public long getValue() {
			return value;
		}
	}

	//Interface for index iterators that can be deregisterd.
	//TODO remove if we remove registerable iterators.
	public interface LongLongIterator<E> extends CloseableIterator<E> {
		
	}

	/**
	 * Interface with special methods for unique indices. 
	 */
	public interface LongLongUIndex extends LongLongIndex {
		LLEntry findValue(long key);
		long removeLong(long key);
	}

	void insertLong(long key, long value);

	/**
	 * If the tree is unique, this simply removes the entry with the given key. If the tree
	 * is not unique, it removes only entries where key AND value match.
	 * @param key
	 * @param value
	 * @return the value.
	 * @throws NoSuchElementException if the key or key/value pair was not found.
	 */
	long removeLong(long key, long value);

	void print();

	/**
	 * Before updating the index, the method checks whether the entry already exists.
	 * In that case the entry is not updated (non-unique is anyway not updated in that case)
	 * and false is returned.
	 * @param key
	 * @param value
	 * @return False if the entry was already used. Otherwise true.
	 */
	boolean insertLongIfNotSet(long key, long value);

	int statsGetLeavesN();

	int statsGetInnerN();

	void clear();

	LongLongIndex.LongLongIterator<LongLongIndex.LLEntry> iterator();

	LongLongIndex.LongLongIterator<LongLongIndex.LLEntry> iterator(long min, long max);

	LongLongIndex.LongLongIterator<LongLongIndex.LLEntry> descendingIterator();

	LongLongIndex.LongLongIterator<LongLongIndex.LLEntry> descendingIterator(long max, long min);

	long getMinKey();

	long getMaxKey();

	void deregisterIterator(LongLongIndex.LongLongIterator<?> it);

	void refreshIterators();

	/**
	 * Write the index (dirty pages only) to disk.
	 * @return pageId of the root page
	 */
	int write();
	
}
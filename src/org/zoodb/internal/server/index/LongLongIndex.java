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
package org.zoodb.internal.server.index;

import java.util.List;
import java.util.NoSuchElementException;

import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.IOResourceProvider;
import org.zoodb.internal.server.StorageChannelOutput;
import org.zoodb.internal.util.CloseableIterator;

/**
 * Interfaces for database indices and their iterators.
 * 
 * 
 * @author Tilmann Zaeschke
 *
 */
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

	//TODO remove?
	//TODO the methods are deprecated because we should avoid too many implementations'
	//TODO check whether this is still a problem for performance
	public interface LLEntryIterator extends LongLongIterator<LLEntry> {
		@Deprecated
		public boolean hasNextULL();

		@Deprecated
		public LLEntry nextULL();

		@Deprecated
		public long nextKey();
	}

	/**
	 * Interface with special methods for unique indices. 
	 */
	public interface LongLongUIndex extends LongLongIndex {
		LLEntry findValue(long key);
		/**
		 * @param key OID
		 * @return the previous value
		 * @throws NoSuchElementException if key is not found
		 */
		long removeLong(long key);

		/**
		 * @param key OID
		 * @param failValue The value to return in case the key has no entry.
		 * @return the previous value
		 */
		long removeLongNoFail(long key, long failValue);
		
		/**
		 * Special method to remove entries. When removing the entry, 
		 * it checks whether other entries in the given range exist. 
		 * If none exist, the value is reported as free page to FSM.
		 * 
		 * In effect, when used in the POS-index, an empty range indicates that there are no more
		 * objects on a given page (pageId=value), therefore the page can be reported as free.
		 * 
		 * @param pos The pos number
		 * @param min min 
		 * @param max max
		 * @return The previous value
		 */
		long deleteAndCheckRangeEmpty(long pos, long min, long max);
	}

	void insertLong(long key, long value);

	/**
	 * If the tree is unique, this simply removes the entry with the given key. If the tree
	 * is not unique, it removes only entries where key AND value match.
	 * @param key The key
	 * @param value The value
	 * @return the value.
	 * @throws NoSuchElementException if the key or key/value pair was not found.
	 */
	long removeLong(long key, long value);

	void print();

	/**
	 * Before updating the index, the method checks whether the entry already exists.
	 * In that case the entry is not updated (non-unique is anyway not updated in that case)
	 * and false is returned.
	 * @param key The key
	 * @param value The value
	 * @return False if the entry was already used. Otherwise true.
	 */
	boolean insertLongIfNotSet(long key, long value);

	int statsGetLeavesN();

	int statsGetInnerN();

	void clear();

	LLEntryIterator iterator();

	LLEntryIterator iterator(long min, long max);

	LongLongIterator<LLEntry> descendingIterator();

	LongLongIterator<LLEntry> descendingIterator(long max, long min);

	long getMinKey();

	long getMaxKey();

	/**
	 * Write the index (dirty pages only) to disk.
	 * @param out Output channel
	 * @return pageId of the root page
	 */
	int write(StorageChannelOutput out);

	long size();
	
	/**
	 * 
	 * @return The data type to which this index is associated.
	 */
	PAGE_TYPE getDataType();

	IOResourceProvider getIO();
	
	/**
	 * 
	 * @return A list of all page IDs used in this index.
	 */
	List<Integer> debugPageIds();

	int statsGetWrittenPagesN();

	boolean isDirty();
}
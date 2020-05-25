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

import java.util.NoSuchElementException;

import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.IOResourceProvider;


/**
 * @author Tilmann Zaeschke
 */
public class PagedUniqueLongLong extends AbstractPagedIndex implements LongLongIndex.LongLongUIndex {
	
	private transient LLIndexPage root;
	
	/**
	 * Constructor for creating new index.
	 * @param dataType The page type 
	 * @param file The file
	 */
	public PagedUniqueLongLong(PAGE_TYPE dataType, IOResourceProvider file) {
		this(dataType, file, 8, 8);
	}

	/**
	 * Constructor for reading index from disk.
	 * @param dataType The page type 
	 * @param file The file
	 * @param pageId The ID of the root page
	 */
	public PagedUniqueLongLong(PAGE_TYPE dataType, IOResourceProvider file, int pageId) {
		this(dataType, file, pageId, 8, 8);
	}

	public PagedUniqueLongLong(PAGE_TYPE dataType, IOResourceProvider file, int pageId, int keySize, int valSize) {
		super(file, true, keySize, valSize, true, dataType);
		root = (LLIndexPage) readRoot(pageId);
	}

	public PagedUniqueLongLong(PAGE_TYPE dataType, IOResourceProvider file, int keySize, int valSize) {
		super(file, true, keySize, valSize, true, dataType);
		//bootstrap index
		root = createPage(null, false);
	}

	@Override
	public final void insertLong(long key, long value) {
		LLIndexPage page = getRoot().locatePageForKeyUnique(key, true);
		page.put(key, value);
	}

	@Override
	public final boolean insertLongIfNotSet(long key, long value) {
		LLIndexPage page = getRoot().locatePageForKeyUnique(key, true);
		if (page.binarySearch(0, page.getNKeys(), key, value) >= 0) {
			return false;
		}
		page.put(key, value);
		return true;
	}

	/**
	 * @param key The key to remove
	 * @return the previous value
	 * @throws NoSuchElementException if key is not found
	 */
	@Override
	public long removeLong(long key) {
		LLIndexPage page = getRoot().locatePageForKeyUnique(key, false);
		if (page == null) {
			throw new NoSuchElementException("Key not found: " + key);
		}
		return page.remove(key);
	}

	/**
	 * @param key The key to remove
	 * @param failValue The value to return in case the key has no entry.
	 * @return the previous value
	 */
	@Override
	public long removeLongNoFail(long key, long failValue) {
		LLIndexPage page = getRoot().locatePageForKeyUnique(key, false);
		if (page == null) {
			return failValue;
		}
		return page.remove(key);
	}

	@Override
	public long removeLong(long key, long value) {
		return removeLong(key);
	}

	@Override
	public LongLongIndex.LLEntry findValue(long key) {
		LLIndexPage page = getRoot().locatePageForKeyUnique(key, false);
		if (page == null) {
			return null;
		}
		return page.getValueFromLeafUnique(key);
	}

	@Override
	LLIndexPage createPage(AbstractIndexPage parent, boolean isLeaf) {
		return new LLIndexPage(this, (LLIndexPage) parent, isLeaf);
	}

	@Override
	protected final LLIndexPage getRoot() {
		return root;
	}

	@Override
	public LLEntryIterator iterator(long min, long max) {
		return new LLIterator(this, min, max);
	}

	@Override
	protected void updateRoot(AbstractIndexPage newRoot) {
		root = (LLIndexPage) newRoot;
	}
	
	@Override
	public String print() {
		return root.print("");
	}

	@Override
	public long getMaxKey() {
		return root.getMax();
	}

	@Override
	public long getMinKey() {
		return root.getMinKey();
	}

	@Override
	public AbstractPageIterator<LongLongIndex.LLEntry> descendingIterator(long max, long min) {
		return new LLDescendingIterator(this, max, min);
	}

	@Override
	public long size() {
		throw new UnsupportedOperationException();
	}

	@Override
	public LLEntryIterator iterator() {
		return iterator(Long.MIN_VALUE, Long.MAX_VALUE);
	}

	@Override
	public AbstractPageIterator<LongLongIndex.LLEntry> descendingIterator() {
		return descendingIterator(Long.MAX_VALUE, Long.MIN_VALUE);
	}

	@Override
	public long deleteAndCheckRangeEmpty(long pos, long min, long max) {
		return getRoot().deleteAndCheckRangeEmpty(pos, min, max);
	}

	/**
	 * This is used in zoodb-server-btree tests.
	 * @return maxLeafN
	 */
	@Override
    public int getMaxLeafN() {
		return maxLeafN;
	}

	/**
	 * This is used in zoodb-server-btree tests.
	 * @return maxInnerN
	 */
	@Override
    public int getMaxInnerN() {
		return maxInnerN;
	}

}

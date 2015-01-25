/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
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

import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.StorageChannel;


/**
 * @author Tilmann Zaeschke
 */
public class PagedUniqueLongLong extends AbstractPagedIndex implements LongLongIndex.LongLongUIndex {
	
	private transient LLIndexPage root;
	
	/**
	 * Constructor for creating new index. 
	 * @param file
	 */
	public PagedUniqueLongLong(DATA_TYPE dataType, StorageChannel file) {
		this(dataType, file, 8, 8);
	}

	/**
	 * Constructor for reading index from disk.
	 */
	public PagedUniqueLongLong(DATA_TYPE dataType, StorageChannel file, int pageId) {
		this(dataType, file, pageId, 8, 8);
	}

	public PagedUniqueLongLong(DATA_TYPE dataType, StorageChannel file, int pageId, int keySize, int valSize) {
		super(file, true, keySize, valSize, true, dataType);
		root = (LLIndexPage) readRoot(pageId);
	}

	public PagedUniqueLongLong(DATA_TYPE dataType, StorageChannel file, int keySize, int valSize) {
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
	 * @param key
	 * @return the previous value
	 * @throws NoSuchElementException if key is not found
	 */
	public long removeLong(long key) {
		LLIndexPage page = getRoot().locatePageForKeyUnique(key, false);
		if (page == null) {
			throw new NoSuchElementException("Key not found: " + key);
		}
		return page.remove(key);
	}

	/**
	 * @param key
	 * @param failValue The value to return in case the key has no entry.
	 * @return the previous value
	 */
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
	public void print() {
		root.print("");
	}

	public long getMaxKey() {
		return root.getMax();
	}

	public long getMinKey() {
		return root.getMinKey();
	}

	public AbstractPageIterator<LongLongIndex.LLEntry> descendingIterator(long max, long min) {
		return new LLDescendingIterator(this, max, min);
	}

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

}

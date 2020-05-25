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
public class PagedLongLong extends AbstractPagedIndex implements LongLongIndex {
	
	private transient LLIndexPage root;
	
	/**
	 * Constructor for creating new index.
	 * @param dataType Page type 
	 * @param file The file
	 */
	public PagedLongLong(PAGE_TYPE dataType, IOResourceProvider file) {
		super(file, true, 8, 8, false, dataType);
		//bootstrap index
		root = createPage(null, false);
	}

	/**
	 * Constructor for reading index from disk.
	 * @param dataType Page type 
	 * @param file The file
	 * @param pageId The ID of the root page
	 */
	public PagedLongLong(PAGE_TYPE dataType, IOResourceProvider file, int pageId) {
		super(file, true, 8, 8, false, dataType);
		root = (LLIndexPage) readRoot(pageId);
	}

	@Override
	public void insertLong(long key, long value) {
		LLIndexPage page = getRoot().locatePageForKey(key, value, true);
		page.insert(key, value);
	}

	@Override
	public boolean insertLongIfNotSet(long key, long value) {
		LLIndexPage page = getRoot().locatePageForKey(key, value, true);
		if (page.binarySearch(0, page.getNKeys(), key, value) >= 0) {
			return false;
		}
		page.insert(key, value);
		return true;
	}

	@Override
	public long removeLong(long key, long value) {
		LLIndexPage page = getRoot().locatePageForKey(key, value, false);
		if (page == null) {
			throw new NoSuchElementException("key not found: " + key + " / " + value);
		}
		return page.remove(key, value);
	}

	@Override
	LLIndexPage createPage(AbstractIndexPage parent, boolean isLeaf) {
		return new LLIndexPage(this, (LLIndexPage) parent, isLeaf);
	}

	@Override
	protected LLIndexPage getRoot() {
		return root;
	}

	@Override
	public LLEntryIterator iterator(long min, long max) {
		return new LLIterator(this, min, max);
	}

	@Override
	public LLEntryIterator iterator() {
		return new LLIterator(this, Long.MIN_VALUE, Long.MAX_VALUE);
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
	public long getMinKey() {
		return root.getMinKey();
	}

	@Override
	public long getMaxKey() {
		return root.getMax();
	}

	@Override
	public AbstractPageIterator<LongLongIndex.LLEntry> descendingIterator(long max, long min) {
		return new LLDescendingIterator(this, max, min);
	}

	@Override
	public AbstractPageIterator<LongLongIndex.LLEntry> descendingIterator() {
		return new LLDescendingIterator(this,
				Long.MAX_VALUE, Long.MIN_VALUE);
	}

	@Override
	public long size() {
		throw new UnsupportedOperationException();
	}
}

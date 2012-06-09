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
package org.zoodb.jdo.internal.server.index;

import java.util.NoSuchElementException;

import org.zoodb.jdo.internal.server.StorageChannel;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;


/**
 * @author Tilmann Zäschke
 */
public class PagedLongLong extends AbstractPagedIndex implements AbstractPagedIndex.LongLongIndex {
	
	private transient LLIndexPage root;
	
	/**
	 * Constructor for creating new index. 
	 * @param raf
	 */
	public PagedLongLong(StorageChannel file) {
		super(file, true, 8, 8, false);
		//bootstrap index
		root = createPage(null, false);
	}

	/**
	 * Constructor for reading index from disk.
	 */
	public PagedLongLong(StorageChannel file, int pageId) {
		super(file, true, 8, 8, false);
		root = (LLIndexPage) readRoot(pageId);
	}

	public void insertLong(long key, long value) {
		LLIndexPage page = getRoot().locatePageForKey(key, value, true);
		page.insert(key, value);
	}

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

	public AbstractPageIterator<LLEntry> iterator(long min, long max) {
		return new LLIterator(this, min, max);
	}

	public AbstractPageIterator<LLEntry> iterator() {
		return new LLIterator(this, Long.MIN_VALUE, Long.MAX_VALUE);
	}

	@Override
	protected void updateRoot(AbstractIndexPage newRoot) {
		root = (LLIndexPage) newRoot;
	}
	
	public void print() {
		root.print("");
	}

	public long getMaxValue() {
		return root.getMax();
	}

	public AbstractPageIterator<LLEntry> descendingIterator(long max, long min) {
		AbstractPageIterator<LLEntry> iter = new LLDescendingIterator(this, max, min);
		return iter;
	}

	public AbstractPageIterator<LLEntry> descendingIterator() {
		AbstractPageIterator<LLEntry> iter = new LLDescendingIterator(this, 
				Long.MAX_VALUE, Long.MIN_VALUE);
		return iter;
	}

}

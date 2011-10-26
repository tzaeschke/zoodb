package org.zoodb.jdo.internal.server.index;

import java.util.Iterator;
import java.util.NoSuchElementException;


import org.zoodb.jdo.internal.server.PageAccessFile;
import org.zoodb.jdo.internal.server.index.AbstractPagedIndex.LongLongIndex;


/**
 * @author Tilmann Zäschke
 */
public class PagedUniqueLongLong extends AbstractPagedIndex implements LongLongIndex {
	
	public static class LLEntry {
		private final long key;
		private final long value;
		LLEntry(long k, long v) {
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
	
	private transient LLIndexPage root;
	
	/**
	 * Constructor for creating new index. 
	 * @param raf
	 */
	public PagedUniqueLongLong(PageAccessFile raf) {
		this(raf, 8, 8);
	}

	/**
	 * Constructor for reading index from disk.
	 */
	public PagedUniqueLongLong(PageAccessFile raf, int pageId) {
		this(raf, pageId, 8, 8);
	}

	public PagedUniqueLongLong(PageAccessFile raf, int pageId, int keySize, int valSize) {
		super(raf, true, keySize, valSize, true);
		root = (LLIndexPage) readRoot(pageId);
	}

	public PagedUniqueLongLong(PageAccessFile raf, int keySize, int valSize) {
		super(raf, true, keySize, valSize, true);
		//bootstrap index
		root = createPage(null, false);
	}

	public final void insertLong(long key, long value) {
		LLIndexPage page = getRoot().locatePageForKeyUnique(key, true);
		page.put(key, value);
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

	public long removeLong(long key, long value) {
		return removeLong(key);
	}

	public LLEntry findValue(long key) {
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

	public AbstractPageIterator<LLEntry> iterator(long min, long max) {
		return new LLIterator(this, min, max);
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

	public Iterator<LLEntry> descendingIterator(long max, long min) {
		return new LLDescendingIterator(this, max, min);
	}

}

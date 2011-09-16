package org.zoodb.jdo.internal.server.index;

import java.util.NoSuchElementException;

import org.zoodb.jdo.internal.server.PageAccessFile;
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
	public PagedLongLong(PageAccessFile raf) {
		super(raf, true, 8, 8, false);
		System.out.println("OidIndex entries per page: " + maxLeafN + " / inner: " + maxInnerN);
		//bootstrap index
		root = createPage(null, false);
	}

	/**
	 * Constructor for reading index from disk.
	 */
	public PagedLongLong(PageAccessFile raf, int pageId) {
		super(raf, true, 8, 8, false);
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

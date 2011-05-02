package org.zoodb.jdo.internal.server.index;

import java.util.Iterator;

import org.zoodb.jdo.internal.server.PageAccessFile;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.ULLDescendingIterator;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.ULLIndexPage;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.ULLIterator;


/**
 * @author Tilmann Zäschke
 */
public class PagedLongLong extends AbstractPagedIndex implements AbstractPagedIndex.LongLongIndex {
	
	private transient ULLIndexPage root;
	
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
		root = (ULLIndexPage) readRoot(pageId);
	}

	public void insertLong(long key, long value) {
		ULLIndexPage page = getRoot().locatePageForKey(key, value, true);
		page.insert(key, value);
	}

	public boolean remove(long key, long value) {
		ULLIndexPage page = getRoot().locatePageForKey(key, value, false);
		if (page == null) {
			return false;
		}
		return page.remove(key, value);
	}

	//TODO remove this
	@Deprecated
	public Iterator<LLEntry> findValues(long key) {
		return iterator(key, key);
	}

	@Override
	ULLIndexPage createPage(AbstractIndexPage parent, boolean isLeaf) {
		return new ULLIndexPage(this, parent, isLeaf);
	}

	@Override
	protected ULLIndexPage getRoot() {
		return root;
	}

	public AbstractPageIterator<LLEntry> iterator(long min, long max) {
		return new ULLIterator(this, min, max);
	}

	public AbstractPageIterator<LLEntry> iterator() {
		return new ULLIterator(this, Long.MIN_VALUE, Long.MAX_VALUE);
	}

	@Override
	protected void updateRoot(AbstractIndexPage newRoot) {
		root = (ULLIndexPage) newRoot;
	}
	
	public void print() {
		root.print();
	}

	public long getMaxValue() {
		return root.getMax();
	}

	public AbstractPageIterator<LLEntry> descendingIterator(long max, long min) {
		AbstractPageIterator<LLEntry> iter = new ULLDescendingIterator(this, max, min);
		return iter;
	}

	public AbstractPageIterator<LLEntry> descendingIterator() {
		AbstractPageIterator<LLEntry> iter = new ULLDescendingIterator(this, 
				Long.MAX_VALUE, Long.MIN_VALUE);
		return iter;
	}

}

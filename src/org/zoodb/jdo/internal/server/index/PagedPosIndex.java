package org.zoodb.jdo.internal.server.index;

import java.util.Iterator;

import org.zoodb.jdo.internal.server.PageAccessFile;
import org.zoodb.jdo.internal.server.index.PagedOidIndex.FilePos;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.ULLIndexPage;

/**
 * See also PagedOidIndex.
 * 
 * TODO Would it be sufficient to have a SET instead of a MAP here? 
 * 
 * @author Tilmann Zäschke
 *
 */
public class PagedPosIndex {

	static class PosOidIterator implements CloseableIterator<FilePos> {

		private final CloseableIterator<LLEntry> iter;
		
		public PosOidIterator(PagedUniqueLongLong root, long minKey, long maxKey) {
			iter = root.iterator(minKey, maxKey);
		}

		@Override
		public boolean hasNext() {
			return iter.hasNext();
		}

		@Override
		public FilePos next() {
			LLEntry e = iter.next();
			return new FilePos(e.value, e.key);
		}

		@Override
		public void remove() {
			iter.remove();
		}
		
		@Override
		public void close() {
			iter.close();
		}
	}
	
	/**
	 * Not really needed for OIDS, but used for testing indices.
	 */
	static class DescendingPosOidIterator implements Iterator<Long> {

		private final Iterator<LLEntry> iter;
		
		public DescendingPosOidIterator(PagedUniqueLongLong root, long maxKey, long minKey) {
			iter = root.descendingIterator(maxKey, minKey);
		}

		@Override
		public boolean hasNext() {
			return iter.hasNext();
		}

		@Override
		public Long next() {
			LLEntry e = iter.next();
			return e.value;
		}

		@Override
		public void remove() {
			iter.remove();
		}
	}
	
	
	private transient PagedUniqueLongLong idx;
	
	/**
	 * Constructor for creating new index. 
	 * @param raf
	 */
	private PagedPosIndex(PageAccessFile raf) {
		idx = new PagedUniqueLongLong(raf);
	}

	/**
	 * Constructor for reading index from disk.
	 */
	private PagedPosIndex(PageAccessFile raf, int pageId) {
		idx = new PagedUniqueLongLong(raf, pageId);
	}

	/**
	 * Constructor for creating new index. 
	 * @param raf
	 */
	public static PagedPosIndex newIndex(PageAccessFile raf) {
		return new PagedPosIndex(raf);
	}
	
	/**
	 * Constructor for reading index from disk.
	 */
	public static PagedPosIndex loadIndex(PageAccessFile raf, int pageId) {
		return new PagedPosIndex(raf, pageId);
	}
	
	public void addPos(int page, int offs, long oid) {
		long newKey = (((long)page) << 32) | (long)offs;
		idx.insertLong(newKey, oid);
	}

	public boolean removePos(int page, int offs) {
		long key = (((long)page) << 32) | (long)offs;
		return idx.removeLong(key);
	}

	public boolean removePos(FilePos pos) {
		return idx.removeLong(pos.getPos());
	}

	public boolean removePosLong(long pos) {
		return idx.removeLong(pos);
	}

	public FilePos findPos(int page, int offs) {
		long key = (((long)page) << 32) | (long)offs;
		LLEntry e = idx.findValue(key);
		return e == null ? null : new FilePos(e.value, e.key);
	}

	public CloseableIterator<FilePos> posIterator() {
		return new PosOidIterator(idx, 0, Long.MAX_VALUE);
	}

	public CloseableIterator<FilePos> posIterator(long min, long max) {
		return new PosOidIterator(idx, min, max);
	}

	public void print() {
		idx.print();
	}

	public long getMaxValue() {
		return idx.getMaxValue();
	}

	public int statsGetLeavesN() {
		return idx.statsGetLeavesN();
	}

	public int statsGetInnerN() {
		return idx.statsGetInnerN();
	}

	public int write() {
		return idx.write();
	}

	public Iterator<Long> descendingIterator() {
		return new DescendingPosOidIterator(idx, Long.MAX_VALUE, 0);
	}

	/**
	 * Checks whether this index contains any positions on the given page.
	 * @param posPage of the form 0xPPPPPPPP00000000, where P denotes the page ID. This is equal
	 * to (pageId << 32). 
	 * @return Whether there are other entries using that page.
	 */
	public boolean containsPage(long posPage) {
		long min = posPage & 0xFFFFFFFF00000000L;
		ULLIndexPage p1 = idx.getRoot().locatePageForKeyUnique(min, false);
		if (p1 == null) {
			return false;
		}
		long max = posPage | 0x00000000FFFFFFFFL;
		//TODO check, is there a difference? should not!
		//long max = min + Config.getFilePageSize();
		if (p1.containsEntryInRangeUnique(min, max)) {
			return true;
		}
		if (p1.getMax() < min) {
			ULLIndexPage p2 = idx.getRoot().locatePageForKeyUnique(max, false);
			return p2.containsEntryInRangeUnique(min, max);
		}
		return false;
	}
}

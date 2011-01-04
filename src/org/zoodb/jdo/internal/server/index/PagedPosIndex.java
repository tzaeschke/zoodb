package org.zoodb.jdo.internal.server.index;

import java.util.Iterator;

import org.zoodb.jdo.internal.server.PageAccessFile;
import org.zoodb.jdo.internal.server.index.PagedOidIndex.FilePos;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;

/**
 * See also PagedOidIndex.
 * 
 * TODO Would it be sufficient to have a SET instead of a MAP here? 
 * 
 * @author Tilmann Zäschke
 *
 */
public class PagedPosIndex {

	@Deprecated
	private static final long MIN_OID = 100;
	
	class PosOidIterator implements Iterator<FilePos> {

		private final Iterator<LLEntry> iter;
		
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
	}
	
	/**
	 * Not really needed for OIDS, but used for testing indices.
	 */
	class DescendingPosOidIterator implements Iterator<Long> {

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
	
	
	private transient long _lastAllocatedInMemory = 100;
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
		_lastAllocatedInMemory = idx.getMaxValue();
		if (_lastAllocatedInMemory < MIN_OID) {
			_lastAllocatedInMemory = MIN_OID;
		}
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
		idx.addLong(newKey, oid);
	}

	public boolean removePos(int page, int offs) {
		long key = (((long)page) << 32) | (long)offs;
		return idx.removeLong(key);
	}

	public boolean removePos(FilePos pos) {
		return idx.removeLong(pos.getPos());
	}

	public FilePos findPos(int page, int offs) {
		long key = (((long)page) << 32) | (long)offs;
		LLEntry e = idx.findValue(key);
		return e == null ? null : new FilePos(e.value, e.key);
	}

	public Iterator<FilePos> posIterator() {
		return new PosOidIterator(idx, 0, Long.MAX_VALUE);
	}

	public void print() {
		idx.print();
	}

	public long getMaxValue() {
		long m = idx.getMaxValue();
		return m > 0 ? m : MIN_OID; 
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
}

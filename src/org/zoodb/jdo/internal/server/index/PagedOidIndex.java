package org.zoodb.jdo.internal.server.index;

import java.util.Iterator;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.jdo.internal.server.PageAccessFile;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;

/**
 * B-Tree like index structure.
 * There are two types of nodes, leaf nodes and inner nodes. Leaf nodes contain key-value pairs.
 * Inner nodes do not contain values. The contain n keys and (n||n+1) page references.
 * 
 * This paged OidIndex can be optimized towards the following properties:
 * - Unique entries
 * - (almost) ordered insertion.
 * - Hopefully (almost) consecutive insertion (unused values only after delete).
 * 
 * Deletion:
 * - Normal BTree deletion: 
 *   if (nEntry < min) then copy entries from prev/next pages
 *   -> if (nEntry < min then) two reads + two writes for every committed update
 *   -> pages are at least half filled -> Reasonable use of space
 *   Improvement: Distribute to prev&next page as soon as possible
 *   -> better use of space
 *   -> always 3 reads
 * - TZ deletion: 
 *   if (prev+this <= nEntries) then merge pages
 *   -> perfectly fine for leaf pages, could be improved to prev+this+next
 *   -> 2(3) reads, 1 writes (2 during merge).
 *   -> can lead to bad trees if used on inner pages and significant deletion in a deep tree;
 *      but still, badness is unlikely to be very bad, no unnecessary leafpages will ever be 
 *      created. TODO: check properly.
 *   -> Improvement: Store leaf size in inner page -> avoids reading neighbouring pages
 *      1 read per delete (1/2 during merge) But: inner pages get smaller. -> short values! 
 *      -> short values can be compressed (cutting of leading 0), because max value depends on
 *         page size, which is fixed. For OID pages: 64/1KB -> 6bit; 4KB->8bit; 16KB->10bit;  
 * - Naive deletion:
 *   Delete leaves only when page is empty
 *   -> Can completely prevent tree shrinkage.
 *   -> 1 read, 1 write
 *    
 * -> So far we go with the naive delete TODO!!!!!!
 * -> Always keep in mind: read access is most likely much more frequent than write access (insert,
 *    update/delete). 
 * -> Also keep in mind: especially inner index nodes are likely to be cached anyway, so they
 *    do not require a re-read. Caching is likely to occur until the index gets much bigger that
 *    memory.
 * -> To support previous statement, and in general: Make garbage collection of leaves easier than
 *    for inner nodes. E.g. reuse (gc-able) page buffers? TODO   
 * 
 * Pages as separate Objects vs hardlinked pages (current implementation).
 * Treating pages as objects is appealing, because most updates require only a single rewrite of the
 * local page and an update of the OID entry. But for OID-indices this is not advisable, because
 * any OID update triggers another OID update until an update fall on a page that is already dirty.
 * Another disadvantage is that values are 64bit, rather than 32bit page IDs. This could be helped
 * by always using page IDs and then again checking all objects on that page (reduced page-read
 * vs. increased CPU usage).  
 * 
 * @author Tilmann Zäschke
 *
 */
public class PagedOidIndex {

	private static final long MIN_OID = 100;
	
	public static final class FilePos {
		final int page;
		final int offs;
		final long oid;
		FilePos(long oid, int page, int offs) {
			this.page = page;
			this.offs = offs;
			this.oid = oid;
		}
		FilePos(long oid, long pos) {
			this.oid = oid;
			this.page = (int)(pos >> 32);
			this.offs = (int)(pos & 0x00000000FFFFFFFF);
		}
		public long getPos() {
			return (((long)page) << 32) | (long)offs;
		}
		public int getPage() {
			return page;
		}
		public int getOffs() {
			return offs;
		}
		public long getOID() {
			return oid;
		}
		@Override
		public String toString() {
			return "FilePos::page=" + page + " ofs=" + offs + " oid=" + oid;
		}
	}
	

	static class OidIterator implements Iterator<FilePos> {

		private final Iterator<LLEntry> iter;
		
		public OidIterator(PagedUniqueLongLong root, long minKey, long maxKey) {
			iter = root.iterator(minKey, maxKey);
		}

		@Override
		public boolean hasNext() {
			return iter.hasNext();
		}

		@Override
		public FilePos next() {
			LLEntry e = iter.next();
			return new FilePos(e.key, e.value);
		}

		@Override
		public void remove() {
			iter.remove();
		}
	}
	
	/**
	 * Not really needed for OIDS, but used for testing indices.
	 */
	static class DescendingOidIterator implements Iterator<FilePos> {

		private final Iterator<LLEntry> iter;
		
		public DescendingOidIterator(PagedUniqueLongLong root, long maxKey, long minKey) {
			iter = root.descendingIterator(maxKey, minKey);
		}

		@Override
		public boolean hasNext() {
			return iter.hasNext();
		}

		@Override
		public FilePos next() {
			LLEntry e = iter.next();
			return new FilePos(e.key, e.value);
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
	public PagedOidIndex(PageAccessFile raf) {
		idx = new PagedUniqueLongLong(raf);
	}

	/**
	 * Constructor for reading index from disk.
	 */
	public PagedOidIndex(PageAccessFile raf, int pageId) {
		idx = new PagedUniqueLongLong(raf, pageId);
		_lastAllocatedInMemory = idx.getMaxValue();
		if (_lastAllocatedInMemory < MIN_OID) {
			_lastAllocatedInMemory = MIN_OID;
		}
	}

	public void addOid(long oid, int schPage, int schOffs) {
		long newVal = (((long)schPage) << 32) | (long)schOffs;
		idx.addLong(oid, newVal);
	}

	public boolean removeOid(long oid) {
		return idx.removeLong(oid);
	}

	/**
	 * 
	 * @param oid
	 * @return FilePos instance or null, if the OID is not known.
	 */
	public FilePos findOid(long oid) {
		LLEntry e = idx.findValue(oid);
		return e == null ? null : new FilePos(e.key, e.value);
	}

	public long[] allocateOids(int oidAllocSize) {
		long l1 = _lastAllocatedInMemory;
		long l2 = l1 + oidAllocSize;

		long[] ret = new long[(int) (l2-l1)];
		for (int i = 0; i < l2-l1; i++ ) {
			ret[i] = l1 + i + 1;
		}
		
		_lastAllocatedInMemory += oidAllocSize;
		if (_lastAllocatedInMemory < 0) {
			throw new JDOFatalDataStoreException("OID overflow after alloc: " + oidAllocSize + 
					" / " + _lastAllocatedInMemory);
		}
		//do not set dirty here!
		return ret;
	}

	public Iterator<FilePos> iterator() {
		return new OidIterator(idx, 0, Long.MAX_VALUE);
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

	public Iterator<FilePos> descendingIterator() {
		return new DescendingOidIterator(idx, Long.MAX_VALUE, 0);
	}
}

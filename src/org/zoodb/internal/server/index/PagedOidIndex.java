/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.index.LongLongIndex.LLEntryIterator;
import org.zoodb.internal.util.DBLogger;

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
 *   if {@code (nEntry < min)} then copy entries from prev/next pages
 *   -- if {@code (nEntry < min then)} two reads + two writes for every committed update
 *   -- pages are at least half filled: Reasonable use of space
 *   Improvement: Distribute to prev and next page as soon as possible
 *   -- better use of space
 *   -- always 3 reads
 * - TZ deletion: 
 *   if {@code (prev+this <= nEntries)} then merge pages
 *   -- perfectly fine for leaf pages, could be improved to prev+this+next
 *   -- 2(3) reads, 1 writes (2 during merge).
 *   -- can lead to bad trees if used on inner pages and significant deletion in a deep tree;
 *      but still, badness is unlikely to be very bad, no unnecessary leafpages will ever be 
 *      created. TODO: check properly.
 *   -- Improvement: Store leaf size in inner page, this avoids reading neighbouring pages
 *      1 read per delete (1/2 during merge) But: inner pages get smaller: short values! 
 *      -- short values can be compressed (cutting of leading 0), because max value depends on
 *         page size, which is fixed. For OID pages: {@code 64/1KB -> 6bit; 4KB->8bit; 16KB->10bit;}  
 * - Naive deletion:
 *   Delete leaves only when page is empty
 *   -- Can completely prevent tree shrinkage.
 *   -- 1 read, 1 write
 *    
 * -- So far we go with the naive delete TODO!!!!!!
 * -- Always keep in mind: read access is most likely much more frequent than write access (insert,
 *    update/delete). 
 * -- Also keep in mind: especially inner index nodes are likely to be cached anyway, so they
 *    do not require a re-read. Caching is likely to occur until the index gets much bigger that
 *    memory.
 * -- To support previous statement, and in general: Make garbage collection of leaves easier than
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
 * @author Tilmann Zaeschke
 *
 */
public class PagedOidIndex {

	private static final long MIN_OID = 100;
	
	public static final class FilePos {
		final int page;
		final int offs;
		final long oid;
		private FilePos(LongLongIndex.LLEntry e) {
			this.oid = e.getKey();
//			this.page = (int)(pos >> 32);
//			this.offs = (int)(pos & 0x000000007FFFFFFF);
			this.page = BitTools.getPage(e.getValue());
			this.offs = BitTools.getOffs(e.getValue());
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

		private final LLEntryIterator iter;
		
		public OidIterator(LongLongIndex.LongLongUIndex root, long minKey, long maxKey) {
			iter = root.iterator(minKey, maxKey);
		}

		@Override
		public boolean hasNext() {
			return iter.hasNextULL();
		}

		@Override
		public FilePos next() {
			LongLongIndex.LLEntry e = iter.nextULL();
			return new FilePos(e);
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

		private final Iterator<LongLongIndex.LLEntry> iter;
		
		public DescendingOidIterator(LongLongIndex.LongLongUIndex root, long maxKey, long minKey) {
			iter = root.descendingIterator(maxKey, minKey);
		}

		@Override
		public boolean hasNext() {
			return iter.hasNext();
		}

		@Override
		public FilePos next() {
			LongLongIndex.LLEntry e = iter.next();
			return new FilePos(e);
		}

		@Override
		public void remove() {
			iter.remove();
		}
	}
	
	
	private transient long lastAllocatedInMemory = MIN_OID;
	private transient LongLongIndex.LongLongUIndex idx;
	
	/**
	 * Constructor for creating new index. 
	 * @param file The file
	 */
	public PagedOidIndex(StorageChannel file) {
		idx = IndexFactory.createUniqueIndex(PAGE_TYPE.OID_INDEX, file);
	}

	/**
	 * Constructor for reading index from disk.
	 * @param file The file
	 * @param pageId The ID of the root page
	 * @param lastUsedOid This parameter indicated the last used OID. It can be derived from 
	 * index.getMaxValue(), because this would allow reuse of OIDs if the latest objects are 
	 * deleted. This might cause a problem if references to the deleted objects still exist.
	 */
	public PagedOidIndex(StorageChannel file, int pageId, long lastUsedOid) {
		idx = IndexFactory.loadUniqueIndex(PAGE_TYPE.OID_INDEX, file, pageId);
		if (lastUsedOid > lastAllocatedInMemory) {
			lastAllocatedInMemory = lastUsedOid;
		}
	}

	public void insertLong(long oid, int schPage, int schOffs) {
		long newVal = (((long)schPage) << 32) | (long)schOffs;
		idx.insertLong(oid, newVal);
		if (oid > lastAllocatedInMemory) {
			lastAllocatedInMemory = oid;
		}
	}

	/**
	 * @param oid key
	 * @return the previous value
	 * @throws NoSuchElementException if key is not found
	 */
	public long removeOid(long oid) {
		return idx.removeLong(oid);
	}

	/**
	 * @param oid key
	 * @param failValue The value to return in case the key has no entry.
	 * @return the previous value
	 */
	public long removeOidNoFail(long oid, long failValue) {
		return idx.removeLongNoFail(oid, failValue);
	}

	/**
	 * 
	 * @param oid The OID to search for
	 * @return FilePos instance or null, if the OID is not known.
	 */
	public FilePos findOid(long oid) {
		LongLongIndex.LLEntry e = idx.findValue(oid);
		return e == null ? null : new FilePos(e);
	}

	public LongLongIndex.LLEntry findOidGetLong(long oid) {
		return idx.findValue(oid);
	}

	public long[] allocateOids(int oidAllocSize) {
		long l1 = lastAllocatedInMemory;
		long l2 = l1 + oidAllocSize;

		long[] ret = new long[(int) (l2-l1)];
		for (int i = 0; i < l2-l1; i++ ) {
			ret[i] = l1 + i + 1;
		}
		
		lastAllocatedInMemory += oidAllocSize;
		if (lastAllocatedInMemory < 0) {
			throw DBLogger.newFatalInternal("OID overflow after alloc: " + oidAllocSize +
					" / " + lastAllocatedInMemory);
		}
		//do not set dirty here!
		return ret;
	}

	public OidIterator iterator() {
		return new OidIterator(idx, 0, Long.MAX_VALUE);
	}

	public void print() {
		idx.print();
	}

	public long getMaxValue() {
		long m = idx.getMaxKey();
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

	public long getLastUsedOid() {
		return lastAllocatedInMemory;
	}
	
	public List<Integer> debugPageIds() {
	    return idx.debugPageIds();
	}

	public void revert(int pageId) {
		idx = IndexFactory.loadUniqueIndex(idx.getDataType(), idx.getStorageChannel(), pageId);
	}
}

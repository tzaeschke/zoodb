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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.IOResourceProvider;
import org.zoodb.internal.server.StorageChannelOutput;
import org.zoodb.internal.server.index.LongLongIndex.LLEntryIterator;

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
 *      but still, badness is unlikely to be very bad, no unnecessary leaf pages will ever be
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
 * Pages as separate Objects vs hard linked pages (current implementation).
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
	
	
	private transient final OidCounter lastAllocatedInMemory = new OidCounter();
	private transient LongLongIndex.LongLongUIndex idx;
	
	/**
	 * Constructor for creating new index. 
	 * @param file The file
	 */
	public PagedOidIndex(IOResourceProvider file) {
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
	public PagedOidIndex(IOResourceProvider file, int pageId, long lastUsedOid) {
		idx = IndexFactory.loadUniqueIndex(PAGE_TYPE.OID_INDEX, file, pageId);
		lastAllocatedInMemory.update(lastUsedOid);
	}

	public void insertLong(long oid, int schPage, int schOffs) {
		long newVal = (((long)schPage) << 32) | (long)schOffs;
		idx.insertLong(oid, newVal);
		lastAllocatedInMemory.update(oid);
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
		//do not set dirty here!
		return lastAllocatedInMemory.allocateOids(oidAllocSize);
	}

	public OidIterator iterator() {
		return new OidIterator(idx, 0, Long.MAX_VALUE);
	}

	public String print() {
		return idx.print();
	}

	public long getMaxValue() {
		long m = idx.getMaxKey();
		return m > 0 ? m : OidCounter.MIN_OID; 
	}

	public int statsGetLeavesN() {
		return idx.statsGetLeavesN();
	}

	public int statsGetInnerN() {
		return idx.statsGetInnerN();
	}

	public int write(StorageChannelOutput out) {
		return idx.write(out);
	}

	public Iterator<FilePos> descendingIterator() {
		return new DescendingOidIterator(idx, Long.MAX_VALUE, 0);
	}

	public long getLastUsedOid() {
		return lastAllocatedInMemory.getLast();
	}
	
	public List<Integer> debugPageIds() {
	    return idx.debugPageIds();
	}

	public void revert(int pageId) {
		idx = IndexFactory.loadUniqueIndex(idx.getDataType(), idx.getIO(), pageId);
	}
}

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

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zoodb.internal.server.DiskIO;
import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.IOResourceProvider;
import org.zoodb.internal.server.StorageChannelInput;
import org.zoodb.internal.server.StorageChannelOutput;
import org.zoodb.internal.server.index.LongLongIndex.LLEntryIterator;
import org.zoodb.internal.util.DBLogger;

/**
 * @author Tilmann Zaeschke
 */
public abstract class AbstractPagedIndex extends AbstractIndex {

	public static final Logger LOGGER = LoggerFactory.getLogger(AbstractPagedIndex.class);

	protected transient final int maxLeafN;
	/** Max number of keys in inner page (there can be max+1 page-refs) */
	protected transient final int maxInnerN;
	//TODO if we ensure that maxXXX is a multiple of 2, then we could scrap the minXXX values
	// minLeafN = maxLeafN >> 1 
	protected transient final int minLeafN;
	// minInnerN = maxInnerN >> 1 
	protected transient final int minInnerN;
	protected int statNLeaves = 0;
	protected int statNInner = 0;
	protected int statNWrittenPages = 0;
	
	protected final int keySize;
	protected final int valSize;
	
	private int modCount = 0;
	private final PAGE_TYPE dataType;
	

	/**
	 * In case this is an existing index, read() should be called afterwards.
	 * Key and value length are used to calculate the man number of entries on a page.
	 * 
	 * @param file The read/write byte stream.
	 * @param isNew Whether this is a new index or existing (i.e. read from disk).
	 * @param keyLen The number of bytes required for the key.
	 * @param valLen The number of bytes required for the value.
	 * @param isUnique Whether the index should be a unique index
	 * @param dataType The page type that should be used for pages of this index
	 */
	public AbstractPagedIndex(IOResourceProvider file, boolean isNew, int keyLen, int valLen,
	        boolean isUnique, PAGE_TYPE dataType) {
		super(file, isNew, isUnique);
		
		int pageSize = getIO().getPageSize();
		this.dataType = dataType;
		
		keySize = keyLen;
		valSize = valLen;
		
		//how many entries fit on one page?
		//
		//all pages: 
		//- 0 byte for flags (isRoot=(root==null)), isLeaf(leaves==0), isDirty(is transient))
		//- // NO! root page: 4 byte  -> Don't store the root! We would have to update it!!!
		//- # leaves: 2 byte
		//- # entries: 2 byte
		//- TODO index-ID (oid)?
		//- TODO index-type (oid for the schema of an index)????
		//
		//middle pages: 
		//- keyLen = keyLen ; refLen = 4byte (pageID)
		//- n values; n+1 references
		//---> n = (PAGE_SIZE - 4 - refLen) / (keyLen + refLen)
		//  
		//leave pages: keyLen = keyLen ; valLen = valLen
		//- n values
		//---> n = (PAGE_SIZE - 4) / (keyLen + valLen)

		final int pageHeader = 4 + DiskIO.PAGE_HEADER_SIZE; // 2 + 2 + general_header
		final int refLen = 4;  //one int for pageID
		// we use only int, so it should round down automatically...
		maxLeafN = (pageSize - pageHeader) / (keyLen + valLen);
		if (maxLeafN * (keyLen + valLen) + pageHeader > pageSize) {
			throw DBLogger.newFatalInternal("Illegal Index size: " + maxLeafN);
		}
		minLeafN = maxLeafN >> 1;
		
		int innerEntrySize = keyLen + refLen;
		if (!isUnique) {
			innerEntrySize += valLen;
		}
		//-2 for short nKeys
		maxInnerN = (pageSize - pageHeader - refLen - 2) / innerEntrySize;
		if (maxInnerN * innerEntrySize + pageHeader + refLen > pageSize) {
			throw DBLogger.newFatalInternal("Illegal Index size: " + maxInnerN);
		}
		minInnerN = maxInnerN >> 1;

	    DBLogger.LOGGER.info("OidIndex entries per page: {} / inner: {}", maxLeafN, maxInnerN);
	}

	abstract AbstractIndexPage createPage(AbstractIndexPage parent, boolean isLeaf);

	public final int write(StorageChannelOutput out) {
		if (!getRoot().isDirty()) {
			markClean(); //This is necessary, even though it shouldn't be ....
			return getRoot().pageId();
		}
		
		int ret = getRoot().write(out);
		markClean();
		return ret;
	}

	/**
	 * Method to preallocate pages for a write command.
	 * @param map map
	 */
	final void preallocatePagesForWriteMap(Map<AbstractIndexPage, Integer> map, 
			FreeSpaceManager fsm) {
		getRoot().createWriteMap(map, fsm);
	}
	
	/**
	 * Special write method that uses only pre-allocated pages.
	 * @param map map
	 * @return the root page Id.
	 */
	final int writeToPreallocated(StorageChannelOutput out, Map<AbstractIndexPage, Integer> map) {
		return getRoot().writeToPreallocated(out, map);
	}
	
	
	protected final AbstractIndexPage readRoot(int pageId) {
		return readPage(pageId, null);
	}
	
	protected abstract AbstractIndexPage getRoot();
	
	final AbstractIndexPage readPage(int pageId, AbstractIndexPage parentPage) {
		if (pageId == 0) {
			throw new IllegalArgumentException();
		}
		//TODO improve compression:
		//no need to store number of entries in leaf pages? Max number is given in code, 
		//actual number is where pageID!=0.
		StorageChannelInput in = getIO().getInputChannel();
		in.seekPageForRead(dataType, pageId);
		int nL = in.readShort();
		AbstractIndexPage newPage;
		if (nL == 0) {
			newPage = createPage(parentPage, true);
			newPage.readData(in);
		} else {
			newPage = createPage(parentPage, false);
			in.noCheckRead(newPage.subPageIds);
			newPage.readKeys(in);
		}
		newPage.setPageId( pageId );  //the page ID is for exampled used to return the page to the FSM
		newPage.setDirty( false );
		getIO().returnInputChannel(in);
		return newPage;
	}

	protected abstract void updateRoot(AbstractIndexPage newRoot);

	public int statsGetInnerN() {
		return statNInner;
	}

	public int statsGetLeavesN() {
		return statNLeaves;
	}
	
	public int statsGetWrittenPagesN() {
		return statNWrittenPages;
	}
	
	final void notifyPageUpdate() {
		modCount++;
	}
	
	public List<Integer> debugPageIds() {
	    ArrayList<Integer> pages = new ArrayList<Integer>();
	    AbstractIndexPage root = getRoot();
	    
	    pages.add(root.pageId());
	    debugGetSubPageIDs(root, pages);
	    
	    return pages;
	}
	
	private void debugGetSubPageIDs(AbstractIndexPage page, ArrayList<Integer> pages) {
	    if (page.isLeaf) {
	        return;
	    }
        for (int i = 0; i <= page.getNKeys(); i++) {
            pages.add(page.subPageIds[i]);
            debugGetSubPageIDs(page.readPage(i), pages);
        }
	}

	public void clear() {
		getRoot().clear();
		file.reportFreePage(getRoot().pageId());
		markDirty();
		this.statNInner = 0;
		this.statNLeaves = 0;
	}
	
	public PAGE_TYPE getDataType() {
		return dataType;
	}

	public int getMaxLeafN() {
		return maxLeafN;
	}

	public int getMaxInnerN() {
		return maxInnerN;
	}

	public void checkValidity(int modCount, long txId) {
		if (this.getIO().getTxId() != txId) {
			throw DBLogger.newUser("This iterator has been invalidated by commit() or rollback().");
		}
		if (this.modCount != modCount) {
			throw new ConcurrentModificationException();
		}
	}

	protected int getModCount() {
		return modCount;
	}

	protected long getTxId() {
		return getIO().getTxId();
	}

	//TODO move this to LongLongIndex?
	//TODO remove?
	abstract LLEntryIterator iterator(long min, long max);
}

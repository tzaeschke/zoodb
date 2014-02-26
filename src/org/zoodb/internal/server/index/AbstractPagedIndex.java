/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.internal.server.DiskIO;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageChannelInput;
import org.zoodb.internal.server.StorageChannelOutput;
import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.index.PagedUniqueLongLong.LLEntry;
import org.zoodb.internal.util.CloseableIterator;
import org.zoodb.internal.util.DBLogger;

/**
 * @author Tilmann Zaeschke
 */
public abstract class AbstractPagedIndex extends AbstractIndex {

	public interface LongLongIndex {
		void insertLong(long key, long value);

		AbstractPageIterator<LLEntry> iterator(long minValue, long maxValue);

		long removeLong(long key, long value);

		void print();

		/**
		 * Before updating the index, the method checks whether the entry already exists.
		 * In that case the entry is not updated (non-unique is anyway not updated in that case)
		 * and false is returned.
		 * @param key
		 * @param value
		 * @return False if the entry was already used. Otherwise true.
		 */
		boolean insertLongIfNotSet(long key, long value);
	}
	
	public abstract static class AbstractPageIterator<E> implements CloseableIterator<E> {
		protected final AbstractPagedIndex ind;
		//TODO use different map to accommodate large numbers of pages?
		//We only have a map with original<->clone associations.
		//There can only be a need to clone a page if it has been modified. If it has been modified,
		//it must have been loaded and should be in the list of loaded leaf-pages.
		private final Map<AbstractIndexPage, AbstractIndexPage> pageClones = 
			new IdentityHashMap<AbstractIndexPage, AbstractIndexPage>();
		
		protected AbstractPageIterator(AbstractPagedIndex ind) {
			this.ind = ind;
			//we have to register the iterator now, because the sub-constructors may already
			//de-register it.
			this.ind.registerIterator(this);
		}
		
		/**
		 * This method checks whether this iterator is interested in the given page, and whether
		 * the iterator already has a copy of that page.
		 * If the page is cloned, then the clone is returned for further us by other iterators.
		 * Sharing is possible, because iterators can not modify pages.
		 * @param page
		 * @param clone or null
		 * @param modcount
		 * @return null or the clone.
		 */
		AbstractIndexPage pageUpdateNotify(AbstractIndexPage page, AbstractIndexPage clone, 
				int modcount) {
			//TODO pageIDs for not-yet committed pages??? -> use original pages as key???
			if (!pageClones.containsKey(page) && pageIsRelevant(page)) {
				if (clone == null) {
					clone = page.newInstance();
					//currently, iterators do not need the parent field. No need to clone it then!
					clone.setParent(null); //pageClones.get(page.parent);
				}
				pageClones.put(page, clone);
				clone.setOriginal( page );
				//maybe we are using it right now?
				replaceCurrentAndStackIfEqual(page, clone);
				
				//now we need to identify the parent and make sure that the parent contains a link
				//to this page. The problem is, that this page may have been loaded only after
				//the parent was cloned, so the parent may not have a reference to this page and
				//may attempt to load it again.
				//But this could fail if this page has been committed in the meantime and changed 
				//its position. Therefore we need to make sure that the parent has a link to this 
				//page.
				// .... why is clone.parent = null???? Why do we set it to nul??
				if (page.getParent() != null && pageClones.get(page.getParent()) != null) {
				    AbstractIndexPage parentClone = pageClones.get(page.getParent());
				    for (int i = 0; i <= parentClone.getNKeys(); i++) {
				        //This is the first time (since this iterator is created) that 'page' is
				        //cloned, therefore the pageId should be correct.
				        if (parentClone.subPageIds[i] == page.pageId()) {
				            //Why do index-tests fail if we don't check for null???
				            if (parentClone.subPages[i] == null) {
				                parentClone.subPages[i] = clone;
				            }
				            break;
				        }
				    }
				}
			}
			//this can still be null
			return clone;
		}

		abstract void replaceCurrentAndStackIfEqual(AbstractIndexPage equal, AbstractIndexPage replace);
		
		protected final void releasePage(AbstractIndexPage oldPage) {
			//just try it, even if it is not in the list.
			if (pageClones.remove(oldPage.getOriginal()) == null && oldPage.getOriginal() != null) {
			    System.out.println("Cloned page not found!");
			}
		}
		
		protected final AbstractIndexPage findPage(AbstractIndexPage currentPage, short pagePos) {
			return currentPage.readPage(pagePos, pageClones);
		}

		/**
		 * Check whether the keys on the page overlap with the min/max/current values of this 
		 * iterator.
		 */
		abstract boolean pageIsRelevant(AbstractIndexPage page);
	
		@Override
		public void close() {
			ind.deregisterIterator(this);
		}
		
		protected boolean isUnique() {
			return ind.isUnique();
		}

		/**
		 * Refresh the iterator (clear COW copies).
		 */
        @Override
		public final void refresh() {
            pageClones.clear();
            reset();
        }
        
        abstract void reset();
	}
	
	

	protected transient final int maxLeafN;
	/** Max number of keys in inner page (there can be max+1 page-refs) */
	protected transient final int maxInnerN;
	//TODO if we ensure that maxXXX is a multiple of 2, then we could scrap the minXXX values
	/** minLeafN = maxLeafN >> 1 */
	protected transient final int minLeafN;
	/** minInnerN = maxInnerN >> 1 */
	protected transient final int minInnerN;
	protected final StorageChannelInput in;
	protected final StorageChannelOutput out;
	protected int statNLeaves = 0;
	protected int statNInner = 0;
	protected int statNWrittenPages = 0;
	
	protected final int keySize;
	protected final int valSize;
	
	//COW stuff
	//TODO make concurrent?!?
	private final WeakHashMap<AbstractPageIterator<?>, Object> iterators = 
		new WeakHashMap<AbstractPageIterator<?>, Object>(); 
	private int modcount = 0;
	private final DATA_TYPE dataType;
	

	/**
	 * In case this is an existing index, read() should be called afterwards.
	 * Key and value length are used to calculate the man number of entries on a page.
	 * 
	 * @param file The read/write byte stream.
	 * @param isNew Whether this is a new index or existing (i.e. read from disk).
	 * @param keyLen The number of bytes required for the key.
	 * @param valLen The number of bytes required for the value.
	 */
	public AbstractPagedIndex(StorageChannel file, boolean isNew, int keyLen, int valLen,
	        boolean isUnique, DATA_TYPE dataType) {
		super(file, isNew, isUnique);
		
		in = file.getReader(false);
		out = file.getWriter(false);
		int pageSize = file.getPageSize();
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
			throw new JDOFatalDataStoreException("Illegal Index size: " + maxLeafN);
		}
		minLeafN = maxLeafN >> 1;
		
		int innerEntrySize = keyLen + refLen;
		if (!isUnique) {
			innerEntrySize += valLen;
		}
		//-2 for short nKeys
		maxInnerN = (pageSize - pageHeader - refLen - 2) / innerEntrySize;
		if (maxInnerN * innerEntrySize + pageHeader + refLen > pageSize) {
			throw new JDOFatalDataStoreException("Illegal Index size: " + maxInnerN);
		}
		minInnerN = maxInnerN >> 1;

	    DBLogger.debugPrintln(1,"OidIndex entries per page: " + maxLeafN + " / inner: " + 
	            maxInnerN);
	}

	abstract AbstractIndexPage createPage(AbstractIndexPage parent, boolean isLeaf);

	public final int write() {
		if (!getRoot().isDirty()) {
			markClean(); //This is necessary, even though it shouldn't be ....
			return getRoot().pageId();
		}
		
		int ret = getRoot().write();
		markClean();
		return ret;
	}

	/**
	 * Method to preallocate pages for a write command.
	 * @param map
	 */
	final void preallocatePagesForWriteMap(Map<AbstractIndexPage, Integer> map, 
			FreeSpaceManager fsm) {
		getRoot().createWriteMap(map, fsm);
	}
	
	/**
	 * Special write method that uses only pre-allocated pages.
	 * @param map
	 * @return the root page Id.
	 */
	final int writeToPreallocated(Map<AbstractIndexPage, Integer> map) {
		return getRoot().writeToPreallocated(map);
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
		in.seekPageForRead(dataType, pageId);
		int nL = in.readShort();
		AbstractIndexPage newPage;
		if (nL == 0) {
			newPage = createPage(parentPage, true);
			newPage.readData();
		} else {
			newPage = createPage(parentPage, false);
			in.noCheckRead(newPage.subPageIds);
			newPage.readKeys();
		}
		newPage.setPageId( pageId );  //the page ID is for exampled used to return the page to the FSM
		newPage.setDirty( false );
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
	
	private AbstractPageIterator<?> registerIterator(AbstractPageIterator<?> iter) {
		iterators.put(iter, new Object());
		return iter;
	}
	
	/**
	 * This is automatically called when the iterator finishes. But it can be called manually
	 * if iteration is aborted before the end is reached.
	 * 
	 * @param iter
	 */
	public void deregisterIterator(AbstractPageIterator<?> iter) {
		iterators.remove(iter);
	}

	final void notifyPageUpdate(AbstractIndexPage page) {
		if (iterators.isEmpty()) {
			//seems stupid, but saves ~10% for some perf tests! 
			return;
		}
        AbstractIndexPage clone = null;
        for (AbstractPageIterator<?> indexIter: iterators.keySet()) {
            clone = indexIter.pageUpdateNotify(page, clone, modcount);
        }
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
		
		for (AbstractPageIterator<?> i: iterators.keySet()) {
			i.close();
		}
		iterators.clear();
	}
	
	final public void refreshIterators() {
        if (iterators.isEmpty()) {
            return;
        }
        Set<AbstractPageIterator<?>> s = new HashSet<AbstractPageIterator<?>>(iterators.keySet());
        for (AbstractPageIterator<?> indexIter: s) {
            indexIter.refresh();
        }
    }

	DATA_TYPE getDataType() {
		return dataType;
	}

}

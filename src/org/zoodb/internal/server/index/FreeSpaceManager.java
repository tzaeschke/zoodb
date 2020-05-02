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
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.IOResourceProvider;
import org.zoodb.internal.server.StorageChannelOutput;
import org.zoodb.internal.server.index.LongLongIndex.LLEntryIterator;
import org.zoodb.internal.util.DBLogger;

/**
 * The free space manager.  
 * 
 * Uses separate index for free pages. Does not use a BitMap, that would only pay out if more 
 * than 1/32 of all pages would be free (based on 4KB page size?).
 * The manager should only return pages that were freed up during previous transactions, but not
 * in the current one. To do so, in the freespace manager, create a new iterator(MIN_INT/MAX_INT) 
 * for every new transaction. The iterator will return only free pages from previous transactions.
 * If (iter.hasNext() == false), use atomic page counter to allocate additional pages.
 * 
 * @author Tilmann Zaeschke
 *
 */
public class FreeSpaceManager {
	
	private transient PagedUniqueLongLong idx;
	private final AtomicInteger lastPage = new AtomicInteger(-1);
	private LLEntryIterator iter;
	
	//Using toAdd/toDelete is purely an optimisation in order to avoid
	//recreating iterators. 
	//TODO A better solution would be to implement iter.remove() and
	//iter.updateValue() and/or let iterators ignore what happens
	//below the current key.
	private final ArrayList<Integer> toAdd = new ArrayList<>();
	private final ArrayList<Integer> toDelete = new ArrayList<>();
	
	//Maximum id transactions whose pages can be reused. This should be global
	private volatile long maxFreeTxId = -1;
	//TODO ThreadLocal???? --> What if commits with in one tx come from different threads?
	private long currentTxId = -1;  //This is local to a transaction
	
	//TODO invert the mapping:
	//Map txId-->pageId!
	//TODO adjust key/value-size in index!
	
	//Currently: Pages to be deleted have an inverted sign: (-txId) 
	
	//Used by the write() method. 
	//Later, we need a map of those, one per session?
	private boolean hasWritingSettled;

	
	/**
	 * Constructor for free space manager.
	 */
	public FreeSpaceManager() {
		//
	}

	/**
	 * Constructor for creating new index. 
	 * @param file The file
	 */
	public void initBackingIndexNew(IOResourceProvider file) {
		if (idx != null) {
			throw new IllegalStateException();
		}
		//8 byte page, 1 byte flag 
		idx = new PagedUniqueLongLong(PAGE_TYPE.FREE_INDEX, file, 4, 8);
	}
	
	/**
	 * Constructor for creating new index. 
	 * @param file The file
	 * @param pageId The page ID of the root page
	 * @param pageCount Current number of pages
	 */
	public void initBackingIndexLoad(IOResourceProvider file, int pageId, int pageCount) {
		if (idx != null) {
			throw new IllegalStateException();
		}
		//8 byte page, 1 byte flag 
		idx = new PagedUniqueLongLong(PAGE_TYPE.FREE_INDEX, file, pageId, 4, 8);
		lastPage.set(pageCount-1);
	}
	
	
	public int write(StorageChannelOutput out) {
		for (Integer l: toDelete) {
			idx.removeLong(l);
		}
		toDelete.clear();

		for (Integer l: toAdd) {
			idx.insertLong(l, currentTxId);
		}
		toAdd.clear();

		//just in case that traversing toAdd required new pages.
		for (Integer l: toDelete) {
			idx.removeLong(l);
		}
		toDelete.clear();

		hasWritingSettled = false;
		
		//repeat until we don't need any more new pages
		Map<AbstractIndexPage, Integer> map = new IdentityHashMap<AbstractIndexPage, Integer>();
		while (!hasWritingSettled) {
			//Reset iterator to avoid ConcurrentModificationException
			//Starting again with '0' should not be a problem. Typically, FSM should
			//anyway contain very few pages with PID_DO_NOT_USE.
			iter.close();
			iter = idx.iterator(0, Long.MAX_VALUE);

			hasWritingSettled = true;
			idx.preallocatePagesForWriteMap(map, this);
			
			for (Integer l: toAdd) {
				idx.insertLong(l, currentTxId);
				hasWritingSettled = false;
			}
			toAdd.clear();
		}

		if (!toDelete.isEmpty()) {
			throw new IllegalStateException();
		}
		
		int pageId = idx.writeToPreallocated(out, map);
		return pageId;
	}

	/**
	 * @return Number of allocated pages in database.
	 */
	public int getPageCount() {
		return lastPage.get() + 1;
	}

	/**
	 * Get a new free page.
	 * @param prevPage Any previous page that is not required anymore, but
	 * can only be re-used in the following transaction.
	 * @return New free page.
	 */
	public int getNextPage(int prevPage) {
		reportFreePage(prevPage);
		
		if (iter.hasNextULL()) {
			//ArrayList<Long> toDelete = new ArrayList<>();
			LongLongIndex.LLEntry e = iter.nextULL();
			long pageId = e.getKey();
			long value = e.getValue();
			
			// do not return pages that are PID_DO_NOT_USE.
			while ((value > maxFreeTxId || value < 0) && iter.hasNextULL()) {
				if (value < 0 && ((-value) <= maxFreeTxId)) {
					//optimisation:, collect in list and remove later?
					toDelete.add((int)pageId);
//					idx.removeLong(pageId);
//					//idx.insertLong(pageId, -currentTxId);
//					iter.close();
//					iter = (LLIterator) idx.iterator(pageId+1, Long.MAX_VALUE);
					//idx.removeLong(pageId, value);
					//TODO or implement iter.remove() ?!
				}
				e = iter.nextULL();
				pageId = e.getKey();
				value = e.getValue();
			}
//			if (!toDelete.isEmpty()) {
//				for (Long l: toDelete) {
//					idx.removeLong(l);
//				}
//			}
			if (value >= 0 && value <= maxFreeTxId) {
				//TODO or implement iter.updateValue() ?!
				//idx.removeLong(pageId);
				idx.insertLong(pageId, -currentTxId);
				iter.close();
				iter = idx.iterator(pageId+1, Long.MAX_VALUE);
				return (int) pageId;
			}
//			if (!toDelete.isEmpty()) {
//				iter.close();
//				iter = (LLIterator) idx.iterator(pageId+1, Long.MAX_VALUE);
//			}
		}
		
		//If we didn't find any we allocate a new page.
		return lastPage.addAndGet(1);
	}

	/**
	 * This method returns a free page without removing it from the FSM. Instead it is labeled
	 * as 'invalid' and will be removed when it is encountered through the normal getNextPage()
	 * method.
	 * Now we make sure that all element of the map are still in the FSM.
	 * Why? Because writing the FSM is tricky because it modifies itself during the process
	 * when it allocates new pages. In theory, it could end up as a infinite loop, when it
	 * repeatedly does the following:
	 * a) allocate page; b) allocating results in page delete and removes it from the FSM;
	 * c) page is returned to the FSM; d) FSM requires a new page and therefore starts over
	 * with a).
	 * Solution: we do not remove pages, but only tag them. More precisely the alloc() in
	 * the index gets them from the FSM, but we don't remove them here, but only later when
	 * they are encountered in the normal getNextPage() method.
	 * 
	 * @param prevPage The page ID of the previous page
	 * @return free page ID
	 */
	public int getNextPageWithoutDeletingIt(int prevPage) {
		reportFreePage(prevPage);

		if (iter.hasNextULL()) {
			LongLongIndex.LLEntry e = iter.nextULL();
			long pageId = e.getKey();
			long value = e.getValue();
			
			// do not return pages that are PID_DO_NOT_USE (i.e. negative value).
			while ((value > maxFreeTxId || value < 0) && iter.hasNextULL()) {
				e = iter.nextULL();
				pageId = e.getKey();
				value = e.getValue();
			}
			if (value >= 0 && value <= maxFreeTxId) {
				//label the page as invalid
				//TODO or implement iter.updateValue() ?!
				idx.insertLong(pageId, -currentTxId);
				iter.close();
				iter = idx.iterator(pageId+1, Long.MAX_VALUE);

				//it should be sufficient to set this only when the new page is taken
				//from the index i.o. the Atomic counter...
				hasWritingSettled = false;
				return (int) pageId;
			}
		}
		
		//If we didn't find any we allocate a new page.
		return lastPage.addAndGet(1);
	}

	public void reportFreePage(int prevPage) {
		if (prevPage > 0) {
			toAdd.add(prevPage);
		}
		//Comment: pages tend to be seemingly reported multiple times, but they are always 
		//PID_DO_NOT_USE pages.
	}
	
	public void notifyCommit() {
		iter.close();
		iter = null;
	}
	
	public void notifyBegin(long newTxId) {
		currentTxId = newTxId;
		
		//TODO not good for multi-session
		maxFreeTxId = currentTxId - 1;
		
		if (iter != null) {
			throw DBLogger.newFatalInternal("Free space manager has unexpected open iterator.");
		}
		
		//Create a new Iterator for the current transaction
		
		//TODO use pageCount i.o. MAX_VALUE???
		//-> No cloning of pages that refer to new allocated disk space
		//-> But checking for isInterestedInPage is also expensive...
		iter = idx.iterator(1, Long.MAX_VALUE);
		
		//TODO optimization:
		//do not create an iterator. Instead implement special method that deletes and returns the
		//first element.
		//This avoids the iterator and the toDelete list. Especially when many many pages are
		//removed, the memory consumption shrinks instead of grows when using an iterator.
		//BUT: The iterator may be faster to return following elements because it knows their 
		//position
	}

    public LLEntryIterator debugIterator() {
        return idx.iterator(Long.MIN_VALUE, Long.MAX_VALUE);
    }

    public List<Integer> debugPageIds() {
        return idx.debugPageIds();
    }

    /**
     * Simply speaking, this returns {@code true} if the given pageId is considered free.
     * Returns {@code true} if the given pageId is in the known (currently free) or newly freed
     * (will be free after next commit) and has not been re-occupied yet.  
     * @param pageId The page ID to check
     * @return Whether the given pageId refers to a free page
     */
    public boolean debugIsPageIdInFreeList(int pageId) {
        return (toAdd.contains(pageId) || idx.findValue(pageId) != null) 
        		&& !toDelete.contains(pageId);
    }

    /**
     * 
     * @return the maximum page id, the page may be free or not.
     */
    public int debugGetMaximumPageId() {
        return lastPage.get();
    }

	public void revert(int pageId, int pageCount) {
		IOResourceProvider file = idx.file;
		idx = null;
		toAdd.clear();
		toDelete.clear();
		iter.close();
		iter = null;
		initBackingIndexLoad(file, pageId, pageCount);
	}
	

	long getTxId() {
		return currentTxId;
	}
}

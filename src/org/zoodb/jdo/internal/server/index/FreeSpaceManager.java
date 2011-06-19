package org.zoodb.jdo.internal.server.index;

import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.zoodb.jdo.internal.server.PageAccessFile;
import org.zoodb.jdo.internal.server.index.AbstractPagedIndex.AbstractIndexPage;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;

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
 * @author Tilmann Zäschke
 *
 */
public class FreeSpaceManager {
	
	private static final int PID_DO_NOT_USE = -1;
	private static final int PID_OK = 1;
	
	private transient PagedUniqueLongLong idx;
	private final AtomicInteger lastPage = new AtomicInteger(-1);
//	private AbstractPageIterator<LLEntry> iter;
	//TODO Long-iterator?? Do we need an iterator at all??
	private Iterator<LLEntry> iter;
	
	private final LinkedList<LLEntry> buffer = new LinkedList<LLEntry>();
	
	private boolean removeInvalidEntries = true;
	
	/**
	 * Constructor for free space manager.
	 */
	public FreeSpaceManager() {
		//
	}

	/**
	 * Constructor for creating new index. 
	 * @param raf
	 */
	public void initBackingIndexNew(PageAccessFile raf) {
		if (idx != null) {
			throw new IllegalStateException();
		}
		idx = new PagedUniqueLongLong(raf);
		//TODO
//		iter = idx.iterator(1, Long.MAX_VALUE);
		Iterator<LLEntry> i = idx.iterator(1, Long.MAX_VALUE);
		while (i.hasNext()) {
			buffer.add(i.next());
		}
		iter = buffer.iterator();
	}
	
	/**
	 * Constructor for creating new index. 
	 * @param raf
	 */
	public void initBackingIndexLoad(PageAccessFile raf, int pageId, int pageCount) {
		if (idx != null) {
			throw new IllegalStateException();
		}
		idx = new PagedUniqueLongLong(raf, pageId);
		lastPage.set(pageCount);
//		iter = idx.iterator(1, Long.MAX_VALUE);//pageCount);
		Iterator<LLEntry> i = idx.iterator(1, Long.MAX_VALUE);
		while (i.hasNext()) {
			buffer.add(i.next());
		}
		iter = buffer.iterator();
	}
	
	
	public int write() {
		Map<AbstractIndexPage, Integer> map = new IdentityHashMap<AbstractIndexPage, Integer>();
		int n = -1;
		removeInvalidEntries = false;
		//repeat until we don't need any more new pages
		while (n != map.size()) {
//			System.out.println("n=" + n + "  / " + map.size());
			n = map.size();
			idx.preallocatePagesForWriteMap(map, this);
			
			//Now we make sure that all element of the map are still in the FSM.
			//Why? Because writing the FSM is tricky because it modifies itself during the process
			//when it allocates new pages. In theory, it could end up as a infinite loop, when it
			//repeatedly does the following:
			//a) allocate page; b) allocating results in page delete and removes it from the FSM;
			//c) page is returned to the FSM; d) FSM requires a new page and therefore starts over
			//with a).
			//Solution: we do not remove pages, but only tag them. More precisely the alloc() in
			//the index gets them from the FSM, but we return them here. The FSM itself does not
			//return pages if they are invalid, but silently discard them later. This later
			//is anywhere outside this method, which is protected by the 'removeInvalidEntries' 
			//flag.
			for (Integer i: map.values()) {
				idx.insertLong(i, PID_DO_NOT_USE);
			}
		}
		
		int pageId = idx.writeToPreallocated(map);
		removeInvalidEntries = true;
		return pageId;
	}

	public int getPageCount() {
		return lastPage.get() + 1;
	}

	/**
	 * This should only be called once, directly after reading the root pages.
	 * @param pageCount
	 */
	public void setPageCount(int pageCount) {
		lastPage.set(pageCount-1);
	}

	public int getNextPage(int prevPage) {
		reportFreePage(prevPage);
		
		if (iter.hasNext()) {
			LLEntry e = iter.next();
			long pageId = e.getKey();//123456; //TODO get from index
			long pageIdValue = e.getValue();//PID_DO_NOT_USE;  //TODO get from index
			
			//TODO do not return pages that are PID_DO_NOT_USE.
			while (pageIdValue == PID_DO_NOT_USE && iter.hasNext()) {
				if (removeInvalidEntries) {
					idx.removeLong(pageId);
				}
				e = iter.next();
				pageId = e.getKey();
				pageIdValue = e.getValue();
			}
			if (pageIdValue != PID_DO_NOT_USE) {
				idx.removeLong(pageId);
				return (int) pageId;
			}
		}
		
		//If we didn't find any we allocate a new page.
		return lastPage.addAndGet(1);
	}

	public void reportFreePage(int prevPage) {
		if (prevPage > 10) {
			idx.insertLong(prevPage, PID_OK);
		}
	}
	
	public void notifyCommit() {
		//TODO
		//idx.deregisterIterator(iter);
		//TODO use pageCount i.o. MAX_VALUE???
		//-> No cloning of pages that refer to new allocated disk space
		//-> But checking for isInterestedInPage is also expensive...
		//iter = idx.iterator(1, Long.MAX_VALUE);
		//TODO
		//buffer.clear();
		if (buffer.isEmpty()) {
			Iterator<LLEntry> i = idx.iterator(1, Long.MAX_VALUE);
			while (i.hasNext()) {
				buffer.add(i.next());
			}
			iter = buffer.iterator();
		}
	}
}

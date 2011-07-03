package org.zoodb.jdo.internal.server.index;

import java.util.NoSuchElementException;

import org.zoodb.jdo.internal.server.PageAccessFile;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.ULLIndexPage;

/**
 * Index that contains all positions of objects as key. If an object spans multiple pages,
 * it gets one entry for each page.
 * The key of each entry is the position. The value of each entry is either the following page
 * (for multi-page objects) or 0 (for single page objects and for he last entry of a multi-page
 * object).
 * 
 * See also PagedOidIndex.
 * 
 * @author Tilmann Zäschke
 *
 */
public class PagedPosIndex {

	/**
	 * This iterator returns only start-pages of objects and skips all intermediate pages.
	 *  
	 * @author Tilmann Zäschke
	 */
	static class ObjectPosIterator implements CloseableIterator<LLEntry> {

		private final CloseableIterator<LLEntry> iter;
		private LLEntry nextE = null;
		
		public ObjectPosIterator(PagedUniqueLongLong root, long minKey, long maxKey) {
			iter = root.iterator(minKey, maxKey);
			if (iter.hasNext()) {
				nextE = iter.next();
			}
		}

		@Override
		public boolean hasNext() {
			return nextE != null;
		}

		@Override
		public LLEntry next() {
			LLEntry ret = nextE;
			if (!iter.hasNext()) {
				//close iterator
				nextE = null;
				iter.close();
				return ret;
			}
			
			//How do we recognize the next object starting point?
			//Find an entry with value=0 (could be the current one) and take the next entry.
			while (nextE.value != 0 && iter.hasNext()) {
				nextE = iter.next();
			}

			if (!iter.hasNext()) {
				//close iterator
				nextE = null;
				iter.close();
				return ret;
			}
			nextE = iter.next();
			return ret;
		}

		@Override
		public void remove() {
			//iter.remove();
			throw new UnsupportedOperationException();
		}
		
		@Override
		public void close() {
			iter.close();
			nextE = null;
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
	
	public void addPos(int page, int offs, long nextPage) {
		long newKey = (((long)page) << 32) | (long)offs;
		idx.insertLong(newKey, nextPage);
	}

	public long removePosLong(long pos) {
		return idx.removeLong(pos);
	}

	public CloseableIterator<LLEntry> iteratorObjects() {
		return new ObjectPosIterator(idx, 0, Long.MAX_VALUE);
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

    public long removePosLongAndCheck(long pos, FreeSpaceManager fsm) {
        ULLIndexPage page = idx.getRoot().locatePageForKeyUnique(pos, false);
        if (page == null) {
            //return false?
            //Can that happen?
            throw new NoSuchElementException("Key not found: " + pos);
        }
        long ret = page.remove(pos);
        
        long min = BitTools.getMinPosInPage(pos);
        long max = BitTools.getMaxPosInPage(pos);
//        int pageId = BitTools.getPage(pos);
//        int offs = BitTools.getOffs(pos);
//        long pos2 = BitTools.getPos(pageId, offs);
        
        //This page may have been deleted, but that should not matter.
        int res = page.containsEntryInRangeUnique(min, max);
        if (res == 0) {
            fsm.reportFreePage((int) (pos >> 32));
            return ret;
        }
        if (res == 1) {
            //entries were found
            return ret;
        }

        //TODO try to exploit the following:
        //if (ret>0) then there are no entries larger than pos.
        //For intermediate pages (not first, not last), there are never other entries.
        //-> All this helps only for large objects
        
        //brute force:
        CloseableIterator<LLEntry> iter = idx.iterator(min, max);
        if (!iter.hasNext()) {
            fsm.reportFreePage((int) (pos >> 32));
        }
        iter.close();
        
        return ret;
    }

//	/**
//	 * Checks whether this index contains any positions on the given page.
//	 * @param posPage of the form 0xPPPPPPPP00000000, where P denotes the page ID. This is equal
//	 * to (pageId << 32). 
//	 * @return Whether there are other entries using that page.
//	 */
//	public boolean containsPage(long posPage) {
//		long min = posPage & 0xFFFFFFFF00000000L;
//		ULLIndexPage p1 = idx.getRoot().locatePageForKeyUnique(min, false);
//		if (p1 == null) {
//			return false;
//		}
//		long max = posPage | 0x000000007FFFFFFFL;
//		//TODO check, is there a difference? should not!
//		//long max = min + Config.getFilePageSize();
//		if (p1.containsEntryInRangeUnique(min, max)) {
//			return true;
//		}
//		if (p1.getMax() < min) {
//			ULLIndexPage p2 = idx.getRoot().locatePageForKeyUnique(max, false);
//			return p2.containsEntryInRangeUnique(min, max);
//		}
//		return false;
//	}
}

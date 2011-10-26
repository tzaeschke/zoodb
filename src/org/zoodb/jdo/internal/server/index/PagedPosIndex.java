package org.zoodb.jdo.internal.server.index;

import java.util.List;
import java.util.NoSuchElementException;

import org.zoodb.jdo.internal.server.PageAccessFile;
import org.zoodb.jdo.internal.util.CloseableIterator;

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

	public static final long MARK_SECONDARY = 0x00000000FFFFFFFFL;
	
	/**
	 * This iterator returns only start-pages of objects and skips all intermediate pages.
	 *  
	 * @author Tilmann Zäschke
	 */
	public static class ObjectPosIterator implements CloseableIterator<Long> {

		private final LLIterator iter;
		private boolean hasNext = true;
		private long nextPos = -1;
		
		public ObjectPosIterator(PagedUniqueLongLong root, long minKey, long maxKey) {
			iter = (LLIterator) root.iterator(minKey, maxKey);
			nextPos();
		}

		@Override
		public boolean hasNext() {
			return hasNextOPI();
		}
		public boolean hasNextOPI() {
			return hasNext;
		}

		@Override
		/**
		 * This next() method returns only primary pages.
		 */
		public Long next() {
			return nextPos();
		}
		
		public long nextPos() {
			//we always only need the key, we return only a long-key
			long ret = nextPos;
			if (!iter.hasNextULL()) {
				//close iterator
				hasNext = false;
				iter.close();
				return ret;
			}
			
			//How do we recognize the next object starting point?
			//The offset of the key is MARK_SECONDARY.
			nextPos = iter.nextKey();
			while (iter.hasNextULL() && BitTools.getOffs(nextPos) == (int)MARK_SECONDARY) {
				nextPos = iter.nextKey();
			}
			if (BitTools.getOffs(nextPos) == (int)MARK_SECONDARY) {
				//close iterator
				hasNext = false;
				iter.close();
				return ret;
			}
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
			hasNext = false;
		}
	}
	
	
	private transient PagedUniqueLongLong idx;
	
	/**
	 * Constructor for creating new index. 
	 * @param raf
	 */
	private PagedPosIndex(PageAccessFile raf) {
		//8 bit starting pos, 4 bit following page
		idx = new PagedUniqueLongLong(raf, 8, 4);
	}

	/**
	 * Constructor for reading index from disk.
	 */
	private PagedPosIndex(PageAccessFile raf, int pageId) {
		//8 bit starting pos, 4 bit following page
		idx = new PagedUniqueLongLong(raf, pageId, 8, 4);
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
	 * @param pageId Set this to MARK_SECONDARY to indicate secondary pages.
	 */
	public static PagedPosIndex loadIndex(PageAccessFile raf, int pageId) {
		return new PagedPosIndex(raf, pageId);
	}
	
	/**
	 * 
	 * @param page
	 * @param offs (long)! To avoid problems when casting -1 from int to long.
	 * @param nextPage
	 */
	public void addPos(int page, long offs, int nextPage) {
		long newKey = (((long)page) << 32) | (long)offs;
		idx.insertLong(newKey, nextPage);
	}

	public long removePosLong(long pos) {
		return idx.removeLong(pos);
	}

	public ObjectPosIterator iteratorObjects() {
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
        LLIndexPage page = idx.getRoot().locatePageForKeyUnique(pos, false);
        if (page == null) {
            //return false?
            //Can that happen?
            throw new NoSuchElementException("Key not found: " + pos);
        }
        long ret = page.remove(pos) << 32L;
        
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
        LLIterator iter = (LLIterator) idx.iterator(min, max);
        if (!iter.hasNextULL()) {
            fsm.reportFreePage((int) (pos >> 32));
        }
        iter.close();
        
        return ret;
    }

    public List<Integer> debugPageIds() {
        return idx.debugPageIds();
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

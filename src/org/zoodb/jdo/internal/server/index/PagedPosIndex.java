/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.jdo.internal.server.index;

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.zoodb.jdo.internal.server.StorageChannel;
import org.zoodb.jdo.internal.server.index.AbstractPagedIndex.AbstractPageIterator;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;
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
	
	public static class ObjectPosIteratorMerger implements CloseableIterator<Long> {
	    private final LinkedList<ObjectPosIterator> il = 
	            new LinkedList<PagedPosIndex.ObjectPosIterator>();
	    private ObjectPosIterator iter = null;
	    
	    void add(ObjectPosIterator iter) {
	        if (this.iter == null) {
	            this.iter = iter;
	        } else {
	            il.add(iter);
	        }
	    }
	    
	    @Override
	    public boolean hasNext() {
            throw new UnsupportedOperationException();
	    }

        public boolean hasNextOPI() {
            if (iter == null) {
                return false;
            }
            if (iter.hasNextOPI()) {
                return true;
            }
            while (!il.isEmpty()) {
                iter.close();
                iter = il.removeFirst();
                if (iter.hasNext){
                    return true;
                }
            }
            return false;
        }

	    @Override
	    public Long next() {
	        throw new UnsupportedOperationException();
	    }

	    public long nextPos() {
	        if (!hasNextOPI()) {
	            throw new NoSuchElementException();
	        }
            return iter.nextPos(); 
	    }
	    
	    @Override
	    public void remove() {
	        throw new UnsupportedOperationException();
	    }

	    @Override
	    public void close() {
	        if (iter != null) {
	            iter.close();
	            iter = null;
	        }
	        for (ObjectPosIterator i: il) {
	            i.close();
	        }
	        il.clear();
	    }

	    @Override
	    public void refresh() {
            if (iter != null) {
                iter.refresh();
            }
            for (ObjectPosIterator i: il) {
                i.refresh();
            }
	    }

	}

	
	/**
	 * This iterator returns only start-pages of objects and skips all intermediate pages.
	 *  
	 * @author Tilmann Zäschke
	 */
	public static class ObjectPosIterator implements CloseableIterator<Long> {

		private LLIterator iter;
		private boolean hasNext = true;
		private long nextPos = -1;
		private final long maxKey;
		
		public ObjectPosIterator(PagedUniqueLongLong root, long minKey, long maxKey) {
			iter = (LLIterator) root.iterator(minKey, maxKey);
			this.maxKey = maxKey;
			nextPos();
		}

        @Override
        public void refresh() {
            if (hasNext) {
                iter = (LLIterator) ((PagedUniqueLongLong)iter.ind).iterator(nextPos, maxKey);
                nextPos();
            }
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
	public PagedPosIndex(StorageChannel file) {
		//8 bit starting pos, 4 bit following page
		idx = new PagedUniqueLongLong(file, 8, 4);
	}

	/**
	 * Constructor for reading index from disk.
	 */
	private PagedPosIndex(StorageChannel file, int pageId) {
		//8 bit starting pos, 4 bit following page
		idx = new PagedUniqueLongLong(file, pageId, 8, 4);
	}

	/**
	 * Constructor for creating new index. 
	 * @param raf
	 */
	public static PagedPosIndex newIndex(StorageChannel file) {
		return new PagedPosIndex(file);
	}
	
	/**
	 * Constructor for reading index from disk.
	 * @param pageId Set this to MARK_SECONDARY to indicate secondary pages.
	 */
	public static PagedPosIndex loadIndex(StorageChannel file, int pageId) {
		return new PagedPosIndex(file, pageId);
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

	public AbstractPageIterator<LLEntry> iteratorPositions() {
		return idx.iterator(0, Long.MAX_VALUE);
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

    public long removePosLongAndCheck(long pos) {
        long min = BitTools.getMinPosInPage(pos);
        long max = BitTools.getMaxPosInPage(pos);
        return idx.getRoot().deleteAndCheckRangeEmpty(pos, min, max) << 32;
    }

    public List<Integer> debugPageIds() {
        return idx.debugPageIds();
    }

	public void clear() {
		idx.clear();
	}

	/**
	 * Abandon COW status and refresh all iterators with latest pages.
	 */
    public void refreshIterators() {
        idx.refreshIterators();
    }

	public long size() {
		return idx.size();
	}
}

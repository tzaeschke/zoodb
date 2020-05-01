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

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.IOResourceProvider;
import org.zoodb.internal.server.StorageChannelOutput;
import org.zoodb.internal.server.index.LongLongIndex.LLEntryIterator;
import org.zoodb.internal.server.index.LongLongIndex.LongLongIterator;
import org.zoodb.internal.server.index.LongLongIndex.LongLongUIndex;
import org.zoodb.internal.util.CloseableIterator;

/**
 * Index that contains all positions of objects as key. If an object spans multiple pages,
 * it gets one entry for each page.
 * The key of each entry is the position. The value of each entry is either the following page
 * (for multi-page objects) or 0 (for single page objects and for he last entry of a multi-page
 * object).
 * 
 * See also PagedOidIndex.
 * 
 * @author Tilmann Zaeschke
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
	}

	
	/**
	 * This iterator returns only start-pages of objects and skips all intermediate pages.
	 *  
	 * @author Tilmann Zaeschke
	 */
	public static class ObjectPosIterator implements CloseableIterator<Long> {

		private LLEntryIterator iter;
		private boolean hasNext = true;
		private long nextPos = -1;
		
		public ObjectPosIterator(LongLongUIndex root, long minKey, long maxKey) {
			iter = root.iterator(minKey, maxKey);
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
	
	
	private transient LongLongUIndex idx;
	
	/**
	 * Constructor for creating new index. 
	 * @param file The file
	 */
	public PagedPosIndex(IOResourceProvider file) {
		//8 bit starting pos, 4 bit following page
		idx = IndexFactory.createUniqueIndex(PAGE_TYPE.POS_INDEX, file, 8, 4);
	}

	/**
	 * Constructor for reading index from disk.
	 */
	private PagedPosIndex(IOResourceProvider file, int pageId) {
		//8 bit starting pos, 4 bit following page
		idx = IndexFactory.loadUniqueIndex(PAGE_TYPE.POS_INDEX, file, pageId, 8, 4);
	}

	/**
	 * Constructor for creating new index. 
	 * @param file The file
	 * @return A new index
	 */
	public static PagedPosIndex newIndex(IOResourceProvider file) {
		return new PagedPosIndex(file);
	}
	
	/**
	 * Constructor for reading index from disk.
	 * @param file The file
	 * @param pageId Set this to MARK_SECONDARY to indicate secondary pages.
	 * @return The loaded index
	 */
	public static PagedPosIndex loadIndex(IOResourceProvider file, int pageId) {
		return new PagedPosIndex(file, pageId);
	}
	
	/**
	 * 
	 * @param page The page to add
	 * @param offs (long)! To avoid problems when casting -1 from int to long.
	 * @param nextPage The following page (in case of cross-border objects)
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

	public LongLongIterator<LongLongIndex.LLEntry> iteratorPositions() {
		return idx.iterator(0, Long.MAX_VALUE);
	}

	public void print() {
		idx.print();
	}

	public long getMaxValue() {
		return idx.getMaxKey();
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

    public long removePosLongAndCheck(long pos) {
        long min = BitTools.getMinPosInPage(pos);
        long max = BitTools.getMaxPosInPage(pos);
        return idx.deleteAndCheckRangeEmpty(pos, min, max) << 32;
    }

    public List<Integer> debugPageIds() {
        return idx.debugPageIds();
    }

	public void clear() {
		idx.clear();
	}

	public long size() {
		return idx.size();
	}
}

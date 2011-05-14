package org.zoodb.jdo.internal.server.index;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Stack;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.jdo.internal.server.PageAccessFile;
import org.zoodb.jdo.internal.server.index.AbstractPagedIndex.LongLongIndex;


/**
 * @author Tilmann Zäschke
 */
public class PagedUniqueLongLong extends AbstractPagedIndex implements LongLongIndex {
	
	public static final boolean DEBUG = true;
	
	public static class LLEntry {
		final long key;
		final long value;
		private LLEntry(long k, long v) {
			key = k;
			value = v;
		}
		public long getKey() {
			return key;
		}
		public long getValue() {
			return value;
		}
	}
	
	private static class IteratorPos {
		IteratorPos(AbstractIndexPage page, short pos) {
			this.page = page;
			this.pos = pos;
		}
		//This is for the iterator, do _not_ use WeakRefs here.
		AbstractIndexPage page;
		short pos;
	}

	/**
	 * Some thoughts on Iterators:
	 * 
	 * JDO has a usecase like this:
	 * Iterator iter = extent.iterator();
	 * while (iter.hasNext()) {
	 * 	   pm.deletePersistent(iter.next());
	 * }
	 * 
	 * That means:
	 * The iterator needs to support deletion without introducing duplicates and without skipping 
	 * objects. It needs to be a perfect iterator.
	 * 
	 * According to the spec 2.2., the extent should contain whatever existed a the time of the 
	 * execution of the query or creation of the iterator (JDO 2.2).
	 * 
	 * So:
	 * - Different sessions should use COW to create locally valid 'copies' of the traversed index.
	 * - Within the same session, iterators should support deletion as described above.
	 * 
	 * To support the deletion locally, there are several option:
	 * - one could use COW as well, which would mean that bidirectional iterators would not work,
	 *   because the iterator iterates over copies of the original list. 
	 *   Basically the above example would work, but deletions ahead of the iterator would not
	 *   be recognized (desirable?). TODO Check spec.
	 * - Alternative: Update iterator with callbacks from index modification.
	 *   This would mean ahead-of-iterator modifications would be recognized (desirable?)
	 *   
	 *    
	 *    
	 *    
	 * Version 2.0:
	 * Iterator stores currentElement and immediately moves to next element. For unique indices
	 * this has the advantage, that the will never be buffer pages created, because the index
	 * is invalidated, as soon as it is created.
	 * 
	 * @author Tilmann Zäschke
	 *
	 */
	static class ULLIterator extends AbstractPageIterator<LLEntry> {

		private ULLIndexPage currentPage = null;
		private short currentPos = 0;
		private final long minKey;
		private final long maxKey;
		private final Stack<IteratorPos> stack = new Stack<IteratorPos>();
		private long nextKey;
		private long nextValue;
		private boolean hasValue = false;
		
		public ULLIterator(AbstractPagedIndex ind, long minKey, long maxKey) {
			super(ind);
			this.minKey = minKey;
			this.maxKey = maxKey;
			this.currentPage = (ULLIndexPage) ind.getRoot();

			findFirstPosInPage();
		}

		@Override
		public boolean hasNext() {
			return hasValue;
		}

		
		private void goToNextPage() {
			releasePage(currentPage);
			IteratorPos ip = stack.pop();
			currentPage = (ULLIndexPage) ip.page;
			currentPos = ip.pos;
			currentPos++;
			
			while (currentPos > currentPage.nEntries) {
				releasePage(currentPage);
				if (stack.isEmpty()) {
					close();
					return;// false;
				}
				ip = stack.pop();
				currentPage = (ULLIndexPage) ip.page;
				currentPos = ip.pos;
				currentPos++;
			}

			while (!currentPage.isLeaf) {
				//we are not on the first page here, so we can assume that pos=0 is correct to 
				//start with

				//read last page
				stack.push(new IteratorPos(currentPage, currentPos));
				currentPage = (ULLIndexPage) findPage(currentPage, currentPos);
				currentPos = 0;
			}
		}
		
		
		private void goToFirstPage() {
			while (!currentPage.isLeaf) {
				//the following is only for the initial search.
				//The stored key[i] is the min-key of the according page[i+1}
			    int pos = Arrays.binarySearch(currentPage.keys, currentPos, currentPage.nEntries, minKey);
			    if (pos >=0) {
			        pos++;
			    } else {
			        pos = -(pos+1);
			    }
			    
			    if (!isUnique()) {
			    	//iterate back
			    	while (pos > 1 && currentPage.keys[pos-2] == minKey) {
			    		pos--;
			    	}
			    }
		    	currentPos = (short)pos;

				//read preceding page
		    	ULLIndexPage newPage = (ULLIndexPage) findPage(currentPage, currentPos);
		    	//if the key matches exactly, there may be elements on the preceding page
		    	if (!isUnique() && pos >= 1 && currentPage.keys[pos-1] == minKey) {
		    		//if there is no previous key, it could also be on previous page 
		    		ULLIndexPage prevPage = (ULLIndexPage) findPage(currentPage, (short) (currentPos-1));
		    		ULLIndexPage dummy = prevPage; 
		    		if (dummy.isLeaf && dummy.keys[dummy.nEntries-1] == minKey) {
			    		newPage = prevPage;
			    		currentPos--;
		    		}
		    		//else
		    		while (!dummy.isLeaf) {
		    			dummy = (ULLIndexPage) findPage(dummy, (short) (dummy.nEntries));
				    	if (dummy.keys[dummy.nEntries-1] == minKey) {
				    		newPage = prevPage;
				    		currentPos--;
				    		break;
				    	}
		    		}
		    	}
				stack.push(new IteratorPos(currentPage, currentPos));
				currentPage = newPage;
				currentPos = 0;
			}
		}
		
		private void gotoPosInPage() {
			//when we get here, we are on a valid page with a valid position 
			//(TODO check for pos after goToPage())
			//we only need to check the value.
			
			nextKey = currentPage.keys[currentPos];
			nextValue = currentPage.values[currentPos];
			hasValue = true;
			currentPos++;
			
			//now progress to next element
			
			//first progress to next page, if necessary.
			if (currentPos >= currentPage.nEntries) {
				goToNextPage();
				if (currentPage == null) {
					return;
				}
			}
			
			//check for invalid value
			if (currentPage.keys[currentPos] > maxKey) {
				close();
			}
		}

		private void findFirstPosInPage() {
			//find first page
			goToFirstPage();

			//find very first element. 
			//TODO use binary search?
			while (currentPos < currentPage.nEntries && currentPage.keys[currentPos] < minKey) {
				currentPos++;
			}
			if (currentPos >= currentPage.nEntries || currentPage.keys[currentPos] > maxKey) {
				close();
				return;
			}
			gotoPosInPage();
		}
		
		
		@Override
		public LLEntry next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			LLEntry e = new LLEntry(nextKey, nextValue);
			if (currentPage == null) {
				hasValue = false;
			} else {
				gotoPosInPage();
			}
			return e;
		}

		@Override
		public void remove() {
			//As defined in the JDO 2.2. spec:
			throw new UnsupportedOperationException();
		}
		
		/**
		 * This method is possibly not be called if the iterator is used in 'for ( : ext) {}' 
		 * constructs! 
		 */
		public void close() {
			// after close() everything should throw NoSuchElementException (see 2.2. spec)
			currentPage = null;
			super.close();
		}

		@Override
		boolean pageIsRelevant(AbstractIndexPage aiPage) {
			if (!hasNext()) {
				return false;
			}
			
			ULLIndexPage page = (ULLIndexPage) aiPage;
			if (page == currentPage) {
				return true;
			}
			if (page.parent == null) {
				//if anything has been cloned, then the root page has been cloned as well.
				return true;
			}
			
			//leaf page?
			if (page.isLeaf) {
				if (currentPage.nEntries == 0) {
					//NO: this must be a new page (isLeaf==true and isEmpty), so we are not interested.
					//YES: This is an empty tree, so we must clone it.
					//Strange: for descendingIterator, this needs to return true. For ascending, 
					//it doesn't seem to matter.
					return false;
				}
				if (maxKey < page.keys[0]
				        || (nextKey > page.keys[page.nEntries-1])
						|| (nextKey == page.keys[page.nEntries-1] && nextValue > page.keys[page.nEntries-1])
				) {
					return false;
				}
				return true;
			}
			
			//inner page
			//specific to forward iterator
			long nextKey2 = findFollowingKeyOrMVInParents(page);
			if (nextKey > nextKey2) {
				return false;
			}
			//check min max
			if (maxKey < findPreceedingKeyOrMVInParents(page)) {
				return false;
			}
			return true;
		}

		/**
		 * This finds the highest key of the previous page. The returned key may or may not be
		 * smaller than the lowest key in the current branch.
		 * @param stackPos
		 * @return Key below min (Unique trees) or key equal to min, or MIN_VALUE, if we are on the
		 * left side of the tree. 
		 */
		private long findPreceedingKeyOrMVInParents(ULLIndexPage child) {
			ULLIndexPage parent = (ULLIndexPage) child.parent;
			//TODO optimize search? E.g. can we use the pos from the stack here????
			int i = 0;
			for (i = 0; i < parent.nEntries; i++) {
				if (parent.leaves[i] == child) {
					break;
				}
			}
			if (i > 0) {
				return parent.keys[i-1];
			}
			//so p==0 here
			if (parent.parent == null) {
				//no root
				return Long.MIN_VALUE;
			}
			return findPreceedingKeyOrMVInParents(parent);
		}
		
		/**
		 * Finds the maximum key of sub-pages by looking at parent pages. The returned value is
		 * probably inclusive, but may no actually be in any child page, in case it has been 
		 * removed. (or are parent updated in that case??? I don't think so. The value would become
		 * more accurate for the lower page, but worse for the higher page. But would that matter?
		 * @param stackPos
		 * @return Probable MAX value or MAX_VALUE, if the highest value is unknown.
		 */
		private long findFollowingKeyOrMVInParents(ULLIndexPage child) {
			ULLIndexPage parent = (ULLIndexPage) child.parent;
			for (int i = 0; i < parent.nEntries; i++) {
				if (parent.leaves[i] == child) {
					return parent.keys[i];
				}
			}
			if (parent.leaves[parent.nEntries] == child) {
				if (parent.parent == null) {
					return Long.MAX_VALUE;
				}
				return findFollowingKeyOrMVInParents(parent);
			}
			throw new JDOFatalDataStoreException("Leaf not found in parent page.");
		}

		@Override
		void replaceCurrentAndStackIfEqual(AbstractIndexPage equal,
				AbstractIndexPage replace) {
			if (currentPage == equal) {
				currentPage = (ULLIndexPage) replace;
				return;
			}
			for (IteratorPos p: stack) {
				if (p.page == equal) {
					p.page = replace;
					return;
				}
			}
		}
	}
	

	/**
	 * Descending iterator.
	 * @author Tilmann Zäschke
	 */
    static class ULLDescendingIterator extends AbstractPageIterator<LLEntry> {

        private ULLIndexPage currentPage = null;
        private short currentPos = 0;
        private final long minKey;
        private final long maxKey;
        private final Stack<IteratorPos> stack = new Stack<IteratorPos>();
        private long nextKey;
        private long nextValue;
        private boolean hasValue = false;
        
        public ULLDescendingIterator(AbstractPagedIndex ind, long maxKey, long minKey) {
            super(ind);
            this.minKey = minKey;
            this.maxKey = maxKey;
            this.currentPage = (ULLIndexPage) ind.getRoot();
            this.currentPos = (short)(currentPage.nEntries-0);
            
            findFirstPosInPage();
        }

        @Override
        public boolean hasNext() {
            return hasValue;
        }

        
        private void goToNextPage() {
            releasePage(currentPage);
            IteratorPos ip = stack.pop();
            currentPage = (ULLIndexPage) ip.page;
            currentPos = ip.pos;
            currentPos--;
            
            while (currentPos < 0) {
                releasePage(currentPage);
                if (stack.isEmpty()) {
                    close();
                    return;// false;
                }
                ip = stack.pop();
                currentPage = (ULLIndexPage) ip.page;
                currentPos = ip.pos;
                currentPos--;
            }

            while (!currentPage.isLeaf) {
                //we are not on the first page here, so we can assume that pos=0 is correct to 
                //start with

                //read last page
                stack.push(new IteratorPos(currentPage, currentPos));
                currentPage = (ULLIndexPage) findPage(currentPage, currentPos);
                currentPos = currentPage.nEntries;
            }
            //leaf page positions are smaller than inner-page positions
            currentPos--;
        }
        
        
        private void goToFirstPage() {
            while (!currentPage.isLeaf) {
                //the following is only for the initial search.
                //The stored value[i] is the min-values of the according page[i+1}
                int pos = Arrays.binarySearch(currentPage.keys, 0, currentPos, maxKey);
                if (pos >=0) {
                    pos++;
                } else {
                    pos = -(pos+1);
                }
                currentPos = (short) pos;
                
			    if (!isUnique()) {
			    	//iterate back
			    	while(pos < currentPage.nEntries && currentPage.keys[pos] == maxKey) {
			    		pos++;
			    	}
			    }

                //read page
			    //Unlike the ascending iterator, we don't needT special non-unique stuff here
                stack.push(new IteratorPos(currentPage, currentPos));
                currentPage = (ULLIndexPage) findPage(currentPage, currentPos);
                currentPos = (short) (currentPage.nEntries);
            }
            //leaf page positions are smaller than inner-page positions
            currentPos--;
        }
        
        private void gotoPosInPage() {
            //when we get here, we are on a valid page with a valid position 
        	//(TODO check for pos after goToPage())
            //we only need to check the value.
            
            nextKey = currentPage.keys[currentPos];
            nextValue = currentPage.values[currentPos];
            hasValue = true;
            currentPos--;
            
            //now progress to next element
            
            //first progress to next page, if necessary.
            if (currentPos < 0) {
                goToNextPage();
                if (currentPage == null) {
                    return;
                }
            }
            
            //check for invalid value
            if (currentPage.keys[currentPos] < minKey) {
                close();
            }
        }

        private void findFirstPosInPage() {
            //find first page
            goToFirstPage();

            //find very first element. 
            //TODO use binary search?
            while (currentPos > 0 && currentPage.keys[currentPos] > maxKey) {
                currentPos--;
            }
            if (currentPos < 0 || currentPage.keys[currentPos] < minKey) {
                close();
                return;
            }
            gotoPosInPage();
        }
        
        
        @Override
        public LLEntry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            LLEntry e = new LLEntry(nextKey, nextValue);
            if (currentPage == null) {
                hasValue = false;
            } else {
                gotoPosInPage();
            }
            return e;
        }

        @Override
        public void remove() {
            //As defined in the JDO 2.2. spec:
            throw new UnsupportedOperationException();
        }
        
        /**
         * This method is possibly not be called if the iterator is used in 'for ( : ext) {}' 
         * constructs! 
         */
        public void close() {
            // after close() everything should throw NoSuchElementException (see 2.2. spec)
            currentPage = null;
            super.close();
        }

        @Override
        boolean pageIsRelevant(AbstractIndexPage aiPage) {
            if (!hasNext()) {
                return false;
            }
            
            ULLIndexPage page = (ULLIndexPage) aiPage;
            if (page == currentPage) {
                return true;
            }
            if (page.parent == null) {
                //if anything has been cloned, then the root page has been cloned as well.
                return true;
            }
            
            //leaf page?
            if (page.isLeaf) {
                if (currentPage.nEntries == 0) {
                    //NO: this must be a new page (isLeaf==true and isEmpty), so we are not interested.
                    //YES: This is an empty tree, so we must clone it.
                    //Strange: for descendingIterator, this needs to return true. For ascending, 
                    //it doesn't seem to matter.
                    return false;
                }
                if (minKey > page.keys[page.nEntries-1]
                        || (nextKey < page.keys[0])
                        || (nextKey == page.keys[0] && nextValue < page.keys[0])
                ) {
                    return false;
                }
                return true;
            }
            
            //inner page
            //specific to forward iterator
            if (nextKey < findPreceedingKeyOrMVInParents(page)) {
                return false;
            }
            //check min max
            if (minKey > findFollowingKeyOrMVInParents(page)) {
                return false;
            }
            return true;
        }

        /**
         * This finds the highest key of the previous page. The returned key may or may not be
         * smaller than the lowest key in the current branch.
         * @param stackPos
         * @return Key below min (Unique trees) or key equal to min, or MIN_VALUE, if we are on the
         * left side of the tree. 
         */
        private long findPreceedingKeyOrMVInParents(ULLIndexPage child) {
            ULLIndexPage parent = (ULLIndexPage) child.parent;
            //TODO optimize search? E.g. can we use the pos from the stack here????
            int i = 0;
            for (i = 0; i < parent.nEntries; i++) {
                if (parent.leaves[i] == child) {
                    break;
                }
            }
            if (i > 0) {
                return parent.keys[i-1];
            }
            //so p==0 here
            if (parent.parent == null) {
                //no root
                return Long.MIN_VALUE;
            }
            return findPreceedingKeyOrMVInParents(parent);
        }
        
        /**
         * Finds the maximum key of sub-pages by looking at parent pages. The returned value is
         * probably inclusive, but may no actually be in any child page, in case it has been 
         * removed. (or are parent updated in that case??? I don't think so. The value would become
         * more accurate for the lower page, but worse for the higher page. But would that matter?
         * @param stackPos
         * @return Probable MAX value or MAX_VALUE, if the highest value is unknown.
         */
        private long findFollowingKeyOrMVInParents(ULLIndexPage child) {
            ULLIndexPage parent = (ULLIndexPage) child.parent;
            for (int i = 0; i < parent.nEntries; i++) {
                if (parent.leaves[i] == child) {
                    return parent.keys[i];
                }
            }
            if (parent.leaves[parent.nEntries] == child) {
                if (parent.parent == null) {
                    return Long.MAX_VALUE;
                }
                return findFollowingKeyOrMVInParents(parent);
            }
            throw new JDOFatalDataStoreException("Leaf not found in parent page.");
        }

        @Override
        void replaceCurrentAndStackIfEqual(AbstractIndexPage equal,
                AbstractIndexPage replace) {
            if (currentPage == equal) {
                currentPage = (ULLIndexPage) replace;
                return;
            }
            for (IteratorPos p: stack) {
                if (p.page == equal) {
                    p.page = replace;
                    return;
                }
            }
        }
    }
    
	
	static class ULLIndexPage extends AbstractIndexPage {
		private final long[] keys;
		//TODO store only pages or also offs? -> test de-ser whole page vs de-ser single obj.
		//     -> especially, objects may not be valid anymore (deleted)! 
		private final long[] values;
		/** number of keys. There are nEntries+1 subPages in any leaf page. */
		private short nEntries;
		
		
		public ULLIndexPage(AbstractPagedIndex ind, AbstractIndexPage parent, boolean isLeaf) {
			super(ind, parent, isLeaf);
			if (isLeaf) {
				keys = new long[ind.maxLeafN];
				values = new long[ind.maxLeafN];
			} else {
				keys = new long[ind.maxInnerN];
				if (ind.isUnique()) {
					values = null;
				} else {
					values = new long[ind.maxInnerN];
				}
			}
		}

		public ULLIndexPage(ULLIndexPage p) {
			super(p);
			keys = p.keys.clone();
			nEntries = p.nEntries;
			if (isLeaf) {
				values = p.values.clone();
			} else {
				if (ind.isUnique()) {
					values = null;
				} else {
					values = new long[ind.maxInnerN];
				}
			}
		}
		
		@Override
		void readData() {
			nEntries = ind.paf.readShort();
			for (int i = 0; i < nEntries; i++) {
				keys[i] = ind.paf.readLong();
				values[i] = ind.paf.readLong();
			}
		}
		
		@Override
		void writeData() {
			ind.paf.writeShort(nEntries);
			for (int i = 0; i < nEntries; i++) {
				ind.paf.writeLong(keys[i]);
				ind.paf.writeLong(values[i]);
			}
		}

		/**
		 * Locate the (first) page that could contain the given key.
		 * In the inner pages, the keys are the minimum values of the following page.
		 * @param key
		 * @return Page for that key
		 */
		public ULLIndexPage locatePageForKeyUnique(long key, boolean allowCreate) {
			if (isLeaf) {
				return this;
			}
			//The stored value[i] is the min-values of the according page[i+1} 
            short pos = (short) Arrays.binarySearch(keys, 0, nEntries, key);
            if (pos >= 0) {
                //pos of matching key
                pos++;
            } else {
                pos = (short) -(pos+1);
                if (pos == nEntries) {
                    //read last page, but does it exist?
                    if (leaves[nEntries]==null && leafPages[nEntries] == 0 && !allowCreate) {
                        return null;
                    }
                }
            }
            //TODO use weak refs
            //read page before that value
            ULLIndexPage page = (ULLIndexPage) readOrCreatePage(pos, allowCreate);
            return page.locatePageForKeyUnique(key, allowCreate);
		}
		
		/**
		 * Locate the (first) page that could contain the given key.
		 * In the inner pages, the keys are the minimum values of the sub-page. The value is
		 * the according minimum value of the first key of the sub-page.
		 * @param key
		 * @return Page for that key
		 */
		public ULLIndexPage locatePageForKey(long key, long value, boolean allowCreate) {
			if (isLeaf) {
				return this;
			}
			//The stored value[i] is the min-values of the according page[i+1} 
            int pos = Arrays.binarySearch(keys, 0, nEntries, key);
            if (pos >= 0) {
                //pos of matching key
                pos++;
            } else {
                pos = -(pos+1);
                if (pos == nEntries) {
                    //read last page, but does it exist?
                    if (leaves[nEntries]==null && leafPages[nEntries] == 0 && !allowCreate) {
                        return null;
                    }
                }
            }
            if (!ind.isUnique()) {
            	int keyPos = pos-1;
            	while (keyPos > 0 && keys[keyPos-1] == key && values[keyPos-1] <= value) {
            		keyPos--;
            	}
            	if (keyPos == 0 && keys[0] == key && values[0] > value) {
            		//becomes pos=0
            		keyPos--;
            	}
            	while (keyPos < nEntries-1 && keys[keyPos+1] == key && values[keyPos+1] <= value) {
            		keyPos++;
            	}
            	pos = keyPos+1;
            }
            //TODO use weak refs
            //read page before that value
            ULLIndexPage page = (ULLIndexPage) readOrCreatePage((short) pos, allowCreate);
            return page.locatePageForKey(key, value, allowCreate);
		}
		
		public LLEntry getValueFromLeaf(long oid) {
			if (!isLeaf) {
				throw new JDOFatalDataStoreException();
			}
			int pos = Arrays.binarySearch(keys, 0, nEntries, oid);
			if (pos >= 0) {
                return new LLEntry( oid, values[pos]);
			}
			//Even if non-unique, if the value could is not on this page, it does not exist.
			return null;
		}


        /**
         * Add an entry at 'key'/'value'. If the PAIR already exists, nothing happens.
         * @param key
         * @param value
         */
		public void insert(long key, long value) {
			put(key, value); 
		}

		public void insertNonUnique(long key, long value, int pos) {
			//pos is >= 0, so we have a key match!
			while (pos > 0 && keys[pos-1] == key && values[pos-1]>value) {
				pos--;
			}
			while (pos < nEntries && keys[pos] == key && values[pos]<value) {
				pos++;
			}
			if (values[pos] == value) {
				//value exists
				return;
			}
			
			//insert value
            markPageDirtyAndClone();
            if (pos < nEntries) {
                System.arraycopy(keys, pos, keys, pos+1, nEntries-pos);
                System.arraycopy(values, pos, values, pos+1, nEntries-pos);
            }
            keys[pos] = key;
            values[pos] = value;
            nEntries++;
            return;
		}

        /**
         * Overwrite the entry at 'key'.
         * @param key
         * @param value
         */
		public void put(long key, long value) {
			if (!isLeaf) {
				throw new JDOFatalDataStoreException();
			}
			
			if (nEntries < ind.maxLeafN) {
				//add locally
	            int pos = Arrays.binarySearch(keys, 0, nEntries, key);
	            //key found? -> pos >=0
	            if (pos >= 0) {
	            	if (!ind.isUnique()) {
	            		insertNonUnique(key, value, pos);
	            		return;
	            	}
	                if (value != values[pos]) {
	                    markPageDirtyAndClone();
	                    values[pos] = value;
	                }
	                return;
	            } 
	            //okay so we add it
	            pos = -(pos+1);
	            markPageDirtyAndClone();
	            if (pos < nEntries) {
	                System.arraycopy(keys, pos, keys, pos+1, nEntries-pos);
	                System.arraycopy(values, pos, values, pos+1, nEntries-pos);
	            }
	            keys[pos] = key;
	            values[pos] = value;
	            nEntries++;
	            return;
			} else {
				//treat page overflow
				ULLIndexPage newP = new ULLIndexPage(ind, parent, true);
				markPageDirtyAndClone();
				int nEntriesToKeep = ind.minLeafN;
				int nEntriesToCopy = ind.maxLeafN - ind.minLeafN;
				if (ind.isUnique()) {
					//find split point such that pages can be completely full
		            int pos = Arrays.binarySearch(keys, 0, nEntries, key + ind.maxLeafN);
		            if (pos < 0) {
		                pos = -(pos+1);
		            }
		            if (pos > nEntriesToKeep) {
		            	nEntriesToKeep = pos;
		            	nEntriesToCopy = ind.maxLeafN - nEntriesToKeep;
		            }
				}
				System.arraycopy(keys, nEntriesToKeep, newP.keys, 0, nEntriesToCopy);
				System.arraycopy(values, nEntriesToKeep, newP.values, 0, nEntriesToCopy);
				nEntries = (short) nEntriesToKeep;
				newP.nEntries = (short) (nEntriesToCopy);
				//New page and min key
				if (ind.isUnique()) {
					if (newP.keys[0] >= key) {
						put(key, value);
					} else {
						newP.put(key, value);
					}
					parent.addSubPage(newP, newP.keys[0], newP.values[0]);
				} else {
					//why doesn't this work???
//					parent.addLeafPage(newP, newP.keys[0], newP.values[0]);
//					locatePageForKey(key, value, false).put(key, value);
					if (newP.keys[0] > key || newP.keys[0]==key && newP.values[0] > value) {
						put(key, value);
					} else {
						newP.put(key, value);
					}
					parent.addSubPage(newP, newP.keys[0], newP.values[0]);				
				}
			}
		}

		void addSubPage(AbstractIndexPage newP, long minKey, long minValue) {
			if (isLeaf) {
				throw new JDOFatalDataStoreException();
			}
			if (nEntries < ind.maxInnerN) {
				//add page here
				//TODO Should we store non-unique values more efficiently? Instead of always storing
				//     the key as well? -> Additional page type for values only? The local value only
				//     being a reference to the value page (inlc offs)? How would efficient insertion
				//     work (shifting values and references to value pages???) ?
				
				
				//For now, we assume a unique index.
				//TODO more efficient search
				int i;
				for ( i = 0; i < nEntries; i++ ) {
					if (keys[i] > minKey) {
						break;
					}
				}
				markPageDirtyAndClone();
				System.arraycopy(keys, i, keys, i+1, nEntries-i);
				System.arraycopy(leaves, i+1, leaves, i+2, nEntries-i);
				System.arraycopy(leafPages, i+1, leafPages, i+2, nEntries-i);
				if (!ind.isUnique()) {
					System.arraycopy(values, i, values, i+1, nEntries-i);
					values[i] = minValue;
				}
				keys[i] = minKey;
				leaves[i+1] = newP;
				newP.parent = this;
				leafPages[i+1] = 0;
				nEntries++;
				return;
			} else {
				//treat page overflow
				ULLIndexPage newInner = (ULLIndexPage) ind.createPage(parent, false);
				
				//TODO use optimized fill ration for unique values, just like for leaves?.
				markPageDirtyAndClone();
				System.arraycopy(keys, ind.minInnerN+1, newInner.keys, 0, nEntries-ind.minInnerN-1);
				if (!ind.isUnique()) {
					System.arraycopy(values, ind.minInnerN+1, newInner.values, 0, nEntries-ind.minInnerN-1);
				}
				System.arraycopy(leaves, ind.minInnerN+1, newInner.leaves, 0, nEntries-ind.minInnerN);
				System.arraycopy(leafPages, ind.minInnerN+1, newInner.leafPages, 0, nEntries-ind.minInnerN);
				newInner.nEntries = (short) (nEntries-ind.minInnerN-1);
				newInner.assignThisAsRootToLeaves();

				if (parent == null) {
					ULLIndexPage newRoot = (ULLIndexPage) ind.createPage(null, false);
					newRoot.leaves[0] = this;
					newRoot.leaves[1] = newInner;
					newRoot.keys[0] = keys[ind.minInnerN];
					if (!ind.isUnique()) {
						newRoot.values[0] = values[ind.minInnerN];
					}
					newRoot.nEntries = 1;
					this.parent = newRoot;
					newInner.parent = newRoot;
					ind.updateRoot(newRoot);
				} else {
					if (ind.isUnique()) {
						parent.addSubPage(newInner, keys[ind.minInnerN], -1);
					} else {
						parent.addSubPage(newInner, keys[ind.minInnerN], values[ind.minInnerN]);
					}
				}
				nEntries = (short) (ind.minInnerN);
				//finally add the leaf to the according page
				if (minKey < newInner.keys[0]) {
					addSubPage(newP, minKey, minValue);
				} else {
					newInner.addSubPage(newP, minKey, minValue);
				}
				return;
			}
		}
		
		public void print() {
			if (isLeaf) {
				System.out.println("Leaf page(" + pageId() + "): n=" + nEntries + " oids=" + 
						Arrays.toString(keys));
				System.out.println("                         " + Arrays.toString(values));
			} else {
				System.out.println("Inner page(" + pageId() + "): n=" + nEntries + " oids=" + 
						Arrays.toString(keys));
				System.out.println("                " + nEntries + " page=" + 
						Arrays.toString(leafPages));
				if (!ind.isUnique()) {
					System.out.println("                " + nEntries + " page=" + 
							Arrays.toString(values));
				}
				System.out.print("[");
				for (int i = 0; i <= nEntries; i++) {
					if (leaves[i] != null) { 
						System.out.print("i=" + i + ": ");
						leaves[i].print();
					}
					else System.out.println("Page not loaded: " + leafPages[i]);
				}
				System.out.println("]");
			}
		}

		public void printLocal() {
			if (isLeaf) {
				System.out.println("Leaf page(" + pageId() + "): n=" + nEntries + " oids=" + 
						Arrays.toString(keys));
				System.out.println("                         " + Arrays.toString(values));
			} else {
				System.out.println("Inner page(" + pageId() + "): n=" + nEntries + " oids=" + 
						Arrays.toString(keys));
				System.out.println("                      " + Arrays.toString(leafPages));
				if (!ind.isUnique()) {
					System.out.println("                      " + Arrays.toString(values));
				}
			}
		}

		protected boolean remove(long oid) {
			if (!ind.isUnique()) {
				throw new IllegalStateException();
			}
			return remove(oid, 0);
		}
		
		protected boolean remove(long oid, long value) {
			//TODO use binary search(?)
			for ( int i = 0; i < nEntries; i++ ) {
				if (keys[i] > oid) {
					return false;
				}
				if (!ind.isUnique()) {
					if (values[i]!=value) {
						//TODO cover possibility of key on following page.
						continue;
					}
				}
				if (keys[i] == oid) {
					// first remove the element
					markPageDirtyAndClone();
					System.arraycopy(keys, i+1, keys, i, nEntries-i-1);
					System.arraycopy(values, i+1, values, i, nEntries-i-1);
					nEntries--;
					if (nEntries == 0) {
						ind.statNLeaves--;
						parent.removeLeafPage(this);
					} else if (nEntries < (ind.maxLeafN >> 1) && (nEntries % 8 == 0)) {
						//The second term prevents frequent reading of previous and following pages.
						
						//now attempt merging this page
						ULLIndexPage prevPage = (ULLIndexPage) parent.getPrevLeafPage(this);
						if (prevPage != null) {
							//We merge only if they all fit on a single page. This means we may read
							//the previous page unnecessarily, but we avoid writing it as long as 
							//possible. TODO find a balance, and do no read prev page in all cases
							if (nEntries + prevPage.nEntries < ind.maxLeafN) {
								//TODO for now this work only for leaves with the same root. We
								//would need to update the min values in the inner nodes.
								prevPage.markPageDirtyAndClone();
								System.arraycopy(keys, 0, prevPage.keys, prevPage.nEntries, nEntries);
								System.arraycopy(values, 0, prevPage.values, prevPage.nEntries, nEntries);
								prevPage.nEntries += nEntries;
								ind.statNLeaves--;
								parent.removeLeafPage(this);
							}
						}
					}
					return true;
				}
			}
			System.out.println("Key not found in page: " + oid);
			print();
			parent.print();
			throw new JDOFatalDataStoreException();
		}


		@Override
		protected void removeLeafPage(AbstractIndexPage indexPage) {
			for (int i = 0; i <= nEntries; i++) {
				if (leaves[i] == indexPage) {
					if (nEntries > 0) { //otherwise we just delete this page
					    //removeLeafPage() is only called by leaves that have already called markPageDirty().
						markPageDirtyAndClone();
						if (i < nEntries) {  //otherwise it's the last element
							if (i > 0) {
								System.arraycopy(keys, i, keys, i-1, nEntries-i);
							} else {
								System.arraycopy(keys, 1, keys, 0, nEntries-1);
							}
							System.arraycopy(leaves, i+1, leaves, i, nEntries-i);
							System.arraycopy(leafPages, i+1, leafPages, i, nEntries-i);
						}
						leafPages[nEntries] = 0;
						leaves[nEntries] = null;
						nEntries--;
					
						//Now try merging
						if (parent == null) {
							return;
						}
						ULLIndexPage prev = (ULLIndexPage) parent.getPrevLeafPage(this);
						if (prev != null) {
							//TODO this is only good for merging inside the same root.
							if ((nEntries % 2 == 0) && (prev.nEntries + nEntries < ind.maxInnerN)) {
							    prev.markPageDirtyAndClone();
								System.arraycopy(keys, 0, prev.keys, prev.nEntries+1, nEntries);
								System.arraycopy(leaves, 0, prev.leaves, prev.nEntries+1, nEntries+1);
								System.arraycopy(leafPages, 0, prev.leafPages, prev.nEntries+1, nEntries+1);
								//find key -> go up or go down????? Up!
								int pos = parent.getPagePosition(this)-1;
								prev.keys[prev.nEntries] = ((ULLIndexPage)parent).keys[pos]; 
								prev.nEntries += nEntries + 1;  //for the additional key
								prev.assignThisAsRootToLeaves();
								ind.statNInner--;
								parent.removeLeafPage(this);
							}
							return;
						}
					} else if (parent != null) {
						ind.statNInner--;
						parent.removeLeafPage(this);
						nEntries--;
					} else {
						//No root and this is a leaf page... -> we do nothing.
						leafPages[0] = 0;
						leaves[0] = null;
						nEntries = 0;
					}
					return;
				}
			}
			System.out.println("this:" + parent);
			this.printLocal();
			System.out.println("leaf:");
			indexPage.printLocal();
			throw new JDOFatalDataStoreException("leaf page not found.");
		}

		public long getMax() {
			if (isLeaf) {
				if (nEntries == 0) {
					return Long.MIN_VALUE;
				}
				return keys[nEntries-1];
			}
			//handle empty indices
			if (nEntries == 0 && leaves[nEntries] == null && leafPages[nEntries] == 0) {
				return Long.MIN_VALUE;
			}
			long max = ((ULLIndexPage)getPageByPos(nEntries)).getMax();
			return max;
		}

		@Override
		void writeKeys() {
			ind._raf.writeShort(nEntries);
			ind._raf.noCheckWrite(keys);
			if (!ind.isUnique()) {
				ind._raf.noCheckWrite(values);
			}
		}

		@Override
		void readKeys() {
			nEntries = ind._raf.readShort();
			ind._raf.noCheckRead(keys);
			if (!ind.isUnique()) {
				ind._raf.noCheckRead(values);
			}
		}

		@Override
		protected AbstractIndexPage newInstance() {
			return new ULLIndexPage(this);
		}
	}
	
	private transient ULLIndexPage root;
	
	/**
	 * Constructor for creating new index. 
	 * @param raf
	 */
	public PagedUniqueLongLong(PageAccessFile raf) {
		super(raf, true, 8, 8, true);
		System.out.println("OidIndex entries per page: " + maxLeafN + " / inner: " + maxInnerN);
		//bootstrap index
		root = createPage(null, false);
	}

	/**
	 * Constructor for reading index from disk.
	 */
	public PagedUniqueLongLong(PageAccessFile raf, int pageId) {
		super(raf, true, 8, 8, true);
		root = (ULLIndexPage) readRoot(pageId);
	}

	public void insertLong(long key, long value) {
		ULLIndexPage page = getRoot().locatePageForKeyUnique(key, true);
		page.put(key, value);
	}

	public boolean removeLong(long key) {
		ULLIndexPage page = getRoot().locatePageForKeyUnique(key, false);
		if (page == null) {
			return false;
		}
		return page.remove(key);
	}

	public boolean removeLong(long key, long value) {
		return removeLong(key);
	}

	public LLEntry findValue(long key) {
		ULLIndexPage page = getRoot().locatePageForKeyUnique(key, false);
		if (page == null) {
			return null;
		}
		return page.getValueFromLeaf(key);
	}

	@Override
	ULLIndexPage createPage(AbstractIndexPage parent, boolean isLeaf) {
		return new ULLIndexPage(this, parent, isLeaf);
	}

	@Override
	protected ULLIndexPage getRoot() {
		return root;
	}

	public AbstractPageIterator<LLEntry> iterator(long min, long max) {
		return new ULLIterator(this, min, max);
	}

	@Override
	protected void updateRoot(AbstractIndexPage newRoot) {
		root = (ULLIndexPage) newRoot;
	}
	
	public void print() {
		root.print();
	}

	public long getMaxValue() {
		return root.getMax();
	}

	public Iterator<LLEntry> descendingIterator(long max, long min) {
		return new ULLDescendingIterator(this, max, min);
	}

}

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

import java.util.NoSuchElementException;

import org.zoodb.internal.server.index.LongLongIndex.LLEntryIterator;

/**
 * Some thoughts on Iterators:
 * 
 * JDO has a use case like this:
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
 * @author Tilmann Zaeschke
 *
 */
class LLIterator extends AbstractPageIterator<LongLongIndex.LLEntry> implements LLEntryIterator {

	private LLIndexPage currentPage;
	private short currentPos = 0;
	private final long minKey;
	private final long maxKey;
	private final LLIteratorStack stack = new LLIteratorStack();
	private long nextKey;
	private long nextValue;
	private boolean hasValue = false;
	
	public LLIterator(AbstractPagedIndex ind, long minKey, long maxKey) {
		super(ind);
		this.minKey = minKey;
		this.maxKey = maxKey;
		this.currentPage = (LLIndexPage) ind.getRoot();

		findFirstPosInPage();
	}

	@Override
	public boolean hasNext() {
		return hasNextULL();
	}
	/**
	 * Dirty trick to avoid delays from finding the correct method.
	 */
	@Override
    public boolean hasNextULL() {
        checkValidity();
		return hasValue;
	}

	
	private void goToNextPage() {
		releasePage(currentPage);
		currentPage = stack.currentPage();
		currentPos = stack.currentPos();
		stack.pop();
		currentPos++;
		
		while (currentPos > currentPage.getNKeys()) {
			releasePage(currentPage);
			if (stack.isEmpty()) {
				close();
				return;// false;
			}
			currentPage = stack.currentPage();
			currentPos = stack.currentPos();
			stack.pop();
			currentPos++;
		}

		while (!currentPage.isLeaf) {
			// We are not on the first page here, so we can assume that pos=0 is correct to
			// start with

			// Read last page
			stack.push(currentPage, currentPos);
			currentPage = (LLIndexPage) findPage(currentPage, currentPos);
			currentPos = 0;
		}
	}
	
	
	private boolean goToFirstPage() {
		while (!currentPage.isLeaf) {
			// The following is only for the initial search.
			// The stored key[i] is the min-key of the according page[i+1}
			int pos2 = currentPage.binarySearch(
					currentPos, currentPage.getNKeys(), minKey, Long.MIN_VALUE);
	    	if (currentPage.getNKeys() == -1) {
				return false;
	    	}
			if (pos2 >= 0) {
		        pos2++;
		    } else {
		        pos2 = -(pos2+1);
		    }
	    	currentPos = (short) pos2;

	    	LLIndexPage newPage = (LLIndexPage) findPage(currentPage, currentPos);
			// Are we on the correct branch?
	    	// We are searching with LONG_MIN value. If the key[] matches exactly, then the
	    	// selected page may not actually contain any valid elements.
	    	// In any case this will be sorted out in findFirstPosInPage()
	    	
			stack.push(currentPage, currentPos);
			currentPage = newPage;
			currentPos = 0;
		}
		return true;
	}
	
	private void gotoPosInPage() {
		// When we get here, we are on a valid page with a valid position
		// (TODO check for pos after goToPage())
		// we only need to check the value.
		
		nextKey = currentPage.getKeys()[currentPos];
		nextValue = currentPage.getValues()[currentPos];
		hasValue = true;
		currentPos++;
		
		// Now progress to next element
		
		// First progress to next page, if necessary.
		if (currentPos >= currentPage.getNKeys()) {
			goToNextPage();
			if (currentPage == null) {
				return;
			}
		}
		
		// Check for invalid value
		if (currentPage.getKeys()[currentPos] > maxKey) {
			close();
		}
	}

	private void findFirstPosInPage() {
		// Find first page
		if (!goToFirstPage()) {
			close();
			return;
		}

		// Find very first element.
		currentPos = (short) currentPage.binarySearch(currentPos, currentPage.getNKeys(), 
				minKey, Long.MIN_VALUE);
		if (currentPos < 0) {
			currentPos = (short) -(currentPos+1);
		}
		
		// Check position
		if (currentPos >= currentPage.getNKeys()) {
			//maybe we walked down the wrong branch?
			goToNextPage();
			if (currentPage == null) {
				close();
				return;
			}
			// Okay, try again.
			currentPos = (short) currentPage.binarySearch(currentPos, currentPage.getNKeys(), 
					minKey, Long.MIN_VALUE);
			if (currentPos < 0) {
				currentPos = (short) -(currentPos+1);
			}
		}
		if (currentPos >= currentPage.getNKeys() 
				|| currentPage.getKeys()[currentPos] > maxKey) {
			close();
			return;
		}
		gotoPosInPage();
	}
	
	
	@Override
	public LongLongIndex.LLEntry next() {
		return nextULL();
	}
	
	/**
	 * Dirty trick to avoid delays from finding the correct method.
	 */
	@Override
    public LongLongIndex.LLEntry nextULL() {
		if (!hasNextULL()) {
			throw new NoSuchElementException();
		}

        LongLongIndex.LLEntry e = new LongLongIndex.LLEntry(nextKey, nextValue);
		if (currentPage == null) {
			hasValue = false;
		} else {
			gotoPosInPage();
		}
		return e;
	}
	
	@Override
    public long nextKey() {
		if (!hasNextULL()) {
			throw new NoSuchElementException();
		}
        checkValidity();

        long ret = nextKey;
		if (currentPage == null) {
			hasValue = false;
		} else {
			gotoPosInPage();
		}
		return ret;
	}


	@Override
	public void remove() {
		// As defined in the JDO 2.2. spec:
		throw new UnsupportedOperationException();
	}
	
	/**
	 * This method is possibly not be called if the iterator is used in 'for ( : ext) {}' 
	 * constructs! 
	 */
	@Override
	public void close() {
		// After close() everything should throw NoSuchElementException (see 2.2. spec)
		currentPage = null;
	}
}
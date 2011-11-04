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

import java.util.ArrayList;
import java.util.NoSuchElementException;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.jdo.internal.server.index.AbstractPagedIndex.AbstractPageIterator;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;

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
class LLIterator extends AbstractPageIterator<LLEntry> {

	static class IteratorPos {
		IteratorPos(LLIndexPage page, short pos) {
			this.page = page;
			this.pos = pos;
		}
		//This is for the iterator, do _not_ use WeakRefs here.
		LLIndexPage page;
		short pos;
	}

	private LLIndexPage currentPage = null;
	private short currentPos = 0;
	private final long minKey;
	private final long maxKey;
	private final ArrayList<IteratorPos> stack = new ArrayList<IteratorPos>(20);
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
	public boolean hasNextULL() {
		return hasValue;
	}

	
	private void goToNextPage() {
		releasePage(currentPage);
		IteratorPos ip = stack.remove(stack.size()-1);
		currentPage = ip.page;
		currentPos = ip.pos;
		currentPos++;
		
		while (currentPos > currentPage.getNKeys()) {
			releasePage(currentPage);
			if (stack.isEmpty()) {
				close();
				return;// false;
			}
			ip = stack.remove(stack.size()-1);
			currentPage = ip.page;
			currentPos = ip.pos;
			currentPos++;
		}

		while (!currentPage.isLeaf) {
			//we are not on the first page here, so we can assume that pos=0 is correct to 
			//start with

			//read last page
			stack.add(new IteratorPos(currentPage, currentPos));
			currentPage = (LLIndexPage) findPage(currentPage, currentPos);
			currentPos = 0;
		}
	}
	
	
	private boolean goToFirstPage() {
		while (!currentPage.isLeaf) {
			//the following is only for the initial search.
			//The stored key[i] is the min-key of the according page[i+1}
			int pos2 = currentPage.binarySearch(
					currentPos, currentPage.getNKeys(), minKey, Long.MIN_VALUE);
	    	if (currentPage.getNKeys() == -1) {
				return false;
	    	}
			if (pos2 >=0) {
		        pos2++;
		    } else {
		        pos2 = -(pos2+1);
		    }
	    	currentPos = (short)pos2;

	    	LLIndexPage newPage = (LLIndexPage) findPage(currentPage, currentPos);
			//are we on the correct branch?
	    	//We are searching with LONG_MIN value. If the key[] matches exactly, then the
	    	//selected page may not actually contain any valid elements.
	    	//In any case this will be sorted out in findFirstPosInPage()
	    	
			stack.add(new IteratorPos(currentPage, currentPos));
			currentPage = newPage;
			currentPos = 0;
		}
		return true;
	}
	
	private void gotoPosInPage() {
		//when we get here, we are on a valid page with a valid position 
		//(TODO check for pos after goToPage())
		//we only need to check the value.
		
		nextKey = currentPage.getKeys()[currentPos];
		nextValue = currentPage.getValues()[currentPos];
		hasValue = true;
		currentPos++;
		
		//now progress to next element
		
		//first progress to next page, if necessary.
		if (currentPos >= currentPage.getNKeys()) {
			goToNextPage();
			if (currentPage == null) {
				return;
			}
		}
		
		//check for invalid value
		if (currentPage.getKeys()[currentPos] > maxKey) {
			close();
		}
	}

	private void findFirstPosInPage() {
		//find first page
		if (!goToFirstPage()) {
			close();
			return;
		}

		//find very first element. 
		currentPos = (short) currentPage.binarySearch(currentPos, currentPage.getNKeys(), 
				minKey, Long.MIN_VALUE);
		if (currentPos < 0) {
			currentPos = (short) -(currentPos+1);
		}
		
		//check position
		if (currentPos >= currentPage.getNKeys()) {
			//maybe we walked down the wrong branch?
			goToNextPage();
			if (currentPage == null) {
				close();
				return;
			}
			//okay, try again.
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
	public LLEntry next() {
		return nextULL();
	}
	
	/**
	 * Dirty trick to avoid delays from finding the correct method.
	 */
	public LLEntry nextULL() {
		if (!hasNextULL()) {
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
	
	public long nextKey() {
		if (!hasNextULL()) {
			throw new NoSuchElementException();
		}
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
		
		LLIndexPage page = (LLIndexPage) aiPage;
		if (page == currentPage) {
			return true;
		}
		if (page.getParent() == null) {
			//if anything has been cloned, then the root page has been cloned as well.
			return true;
		}
		
		//leaf page?
		if (page.isLeaf) {
			if (page.getNKeys() == 0) {
				//NO: this must be a new page (isLeaf==true and isEmpty), so we are not 
				//    interested.
				//YES: This is an empty tree, so we must clone it.
				//Strange: for descendingIterator, this needs to return true. For ascending, 
				//it doesn't seem to matter.
				return false;
			}
			long lastKey = page.getKeys()[page.getNKeys() - 1];
			if (maxKey < page.getKeys()[0]
			        || (nextKey > lastKey)
					|| (nextKey == lastKey && nextValue > lastKey)
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
	private long findPreceedingKeyOrMVInParents(LLIndexPage child) {
		LLIndexPage parent = child.getParent();
		//TODO optimize search? E.g. can we use the pos from the stack here????
		int i = 0;
		for (i = 0; i < parent.getNKeys(); i++) {
			if (parent.subPages[i] == child) {
				break;
			}
		}
		if (i > 0) {
			return parent.getKeys()[i-1];
		}
		//so p==0 here
		if (parent.getParent() == null) {
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
	private long findFollowingKeyOrMVInParents(LLIndexPage child) {
		LLIndexPage parent = child.getParent();
		for (int i = 0; i < parent.getNKeys(); i++) {
			if (parent.subPages[i] == child) {
				return parent.getKeys()[i];
			}
		}
		if (parent.subPages[parent.getNKeys()] == child) {
			if (parent.getParent() == null) {
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
			currentPage = (LLIndexPage) replace;
			return;
		}
		for (IteratorPos p: stack) {
			if (p.page == equal) {
				p.page = (LLIndexPage) replace;
				return;
			}
		}
	}
}
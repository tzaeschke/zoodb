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
import java.util.NoSuchElementException;

import org.zoodb.internal.server.index.AbstractPagedIndex.AbstractPageIterator;
import org.zoodb.internal.server.index.AbstractPagedIndex.LLEntry;
import org.zoodb.internal.server.index.LLIterator.IteratorPos;
import org.zoodb.internal.util.DBLogger;

/**
 * Descending iterator.
 * @author Tilmann Zaeschke
 */
class LLDescendingIterator extends AbstractPageIterator<LLEntry> {

    private LLIndexPage currentPage = null;
    private short currentPos = 0;
    private final long minKey;
    private long maxKey;
    private final ArrayList<IteratorPos> stack = new ArrayList<IteratorPos>(20);
    private long nextKey;
    private long nextValue;
    private boolean hasValue = false;
    
    public LLDescendingIterator(AbstractPagedIndex ind, long maxKey, long minKey) {
        super(ind);
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.currentPage = (LLIndexPage) ind.getRoot();
        this.currentPos = (short)(currentPage.getNKeys()-0);
        
        findFirstPosInPage();
    }

    @Override
    public boolean hasNext() {
        return hasValue;
    }

    @Override
    protected final void reset() {
        stack.clear();
        this.maxKey = nextKey;
        this.currentPage = (LLIndexPage) ind.getRoot();
        this.currentPos = (short)(currentPage.getNKeys()-0);
           
        findFirstPosInPage();
    }
    
    private void goToNextPage() {
        releasePage(currentPage);
        IteratorPos ip = stack.remove(stack.size()-1);
        currentPage = ip.page;
        currentPos = ip.pos;
        currentPos--;
        
        while (currentPos < 0) {
            releasePage(currentPage);
            if (stack.isEmpty()) {
                close();
                return;// false;
            }
            ip = stack.remove(stack.size()-1);
            currentPage = ip.page;
            currentPos = ip.pos;
            currentPos--;
        }

        while (!currentPage.isLeaf) {
            //we are not on the first page here, so we can assume that pos=0 is correct to 
            //start with

            //read last page
            stack.add(new IteratorPos(currentPage, currentPos));
            currentPage = (LLIndexPage) findPage(currentPage, currentPos);
            currentPos = currentPage.getNKeys();
        }
        //leaf page positions are smaller than inner-page positions
        currentPos--;
    }
    
    
    private boolean goToFirstPage() {
		while (!currentPage.isLeaf) {
            //the following is only for the initial search.
            //The stored value[i] is the min-values of the according page[i+1}
            int pos = currentPage.binarySearch(0, currentPos, maxKey, Long.MAX_VALUE);
            if (currentPage.getNKeys() == -1) {
            	return false;
            }
            if (pos >=0) {
                pos++;
            } else {
                pos = -(pos+1);
            }
            currentPos = (short) pos;
            
            //read page
		    //Unlike the ascending iterator, we don't need special non-unique stuff here
            stack.add(new IteratorPos(currentPage, currentPos));
            currentPage = (LLIndexPage) findPage(currentPage, currentPos);
            currentPos = currentPage.getNKeys();
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
        if (currentPage.getKeys()[currentPos] < minKey) {
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
		int pos = (short) currentPage.binarySearch(0, currentPage.getNKeys(), maxKey, Long.MAX_VALUE);
        if (pos < 0) {
            pos = -(pos+2); //-(pos+1);
        }
        currentPos = (short) pos;
        
        //check pos
        if (currentPos < 0 || currentPage.getKeys()[currentPos] < minKey) {
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
    @Override
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
                //NO: this must be a new page (isLeaf==true and isEmpty), so we are not interested.
                //YES: This is an empty tree, so we must clone it.
                //Strange: for descendingIterator, this needs to return true. For ascending, 
                //it doesn't seem to matter.
                return false;
            }
            long[] keys = page.getKeys();
            if (minKey > keys[page.getNKeys()-1]
                    || (nextKey < keys[0])
                    || (nextKey == keys[0] && nextValue < keys[0])
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
        throw DBLogger.newFatal("Leaf not found in parent page.");
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
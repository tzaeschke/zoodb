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
import java.util.NoSuchElementException;

import org.zoodb.internal.server.index.LLIterator.IteratorPos;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;

/**
 * Descending iterator.
 * @author Tilmann Zaeschke
 */
class LLDescendingIterator extends AbstractPageIterator<LongLongIndex.LLEntry> {

    private LLIndexPage currentPage = null;
    private short currentPos = 0;
    private final long minKey;
    private final long maxKey;
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
        checkValidity();
        return hasValue;
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
    public LongLongIndex.LLEntry next() {
        if (!hasNext()) {
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
    }
}
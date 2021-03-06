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
package org.zoodb.internal.util;

import java.util.ArrayList;
import java.util.NoSuchElementException;

import org.zoodb.internal.server.index.LongLongIndex;

/**
 * This merging iterator merges multiple iterators into a single one.
 * The returned results are ordered in the natural (Java) order.
 * The iterator expects the sub-iterators to be sorted.
 * 
 * TODO For queries across multiple nodes, merge asynchronously by running sub-iterators in 
 * different threads and merge the result as they arrive.
 * 
 * @author Tilmann Zaeschke
 *
 */
public class OrderedMergeIterator implements CloseableIterator<LongLongIndex.LLEntry> {

	private final ArrayList<CloseableIterator<LongLongIndex.LLEntry>> iterators = 
			new ArrayList<CloseableIterator<LongLongIndex.LLEntry>>();
	private final ArrayList<LongLongIndex.LLEntry> currentValues;
	private LongLongIndex.LLEntry current; 
	private final IteratorRegistry registry;
	private boolean isClosed = false;
	
    public OrderedMergeIterator(CloseableIterator<LongLongIndex.LLEntry>[] iterators) {
    	this(null, iterators);
    }

    public OrderedMergeIterator(IteratorRegistry registry, CloseableIterator<LongLongIndex.LLEntry>[] iterators) {
        this.registry = registry;
        if (registry != null) {
        	registry.registerResource(this);
        }
        //init values
        currentValues = new ArrayList<LongLongIndex.LLEntry>();
        for (int i = 0; i < iterators.length; i++) {
        	CloseableIterator<LongLongIndex.LLEntry> iter = iterators[i];
        	if (iter.hasNext()) {
            	this.iterators.add(iter);
        		currentValues.add(iter.next());
        	}
        }
        //find smallest value
        if (!currentValues.isEmpty()) {
	        int currentPos = 0;
	        current = currentValues.get(0);
	        for (int i = 1; i < currentValues.size(); i++) {
	        	if (current.getKey() > currentValues.get(i).getKey()) {
	        		currentPos = i;
	        		current = currentValues.get(i);
	        	}
	        }
	        //refill
	        getNext(currentPos);
        }
    }
    
    private void getNext(int currentPos) {
    	CloseableIterator<LongLongIndex.LLEntry> iter = iterators.get(currentPos);
    	if (iter.hasNext()) {
    		currentValues.set(currentPos, iter.next());
    	} else {
    		iter.close();
    		iterators.remove(currentPos);
    		currentValues.remove(currentPos);
    	}
	}

    @Override
	public boolean hasNext() {
    	if (isClosed) {
    		throw DBLogger.newUser("This iterator has been closed.");
    	}
		if (current == null) {
			return false;
		}
		return true;
	}

	@Override
	public LongLongIndex.LLEntry next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		
		LongLongIndex.LLEntry dummy = current;
		
        //find smallest value
        if (currentValues.isEmpty()) {
        	current = null;
        } else {
	        int currentPos = 0;
	        current = currentValues.get(0);
	        for (int i = 1; i < currentValues.size(); i++) {
	        	if (current.getKey() > currentValues.get(i).getKey()) {
	        		currentPos = i;
	        		current = currentValues.get(i);
	        	}
	        }
	        //refill
	        getNext(currentPos);
        }
		return dummy;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
		//current.remove();
	}

	@Override
	public void close() {
		isClosed = true;
		for (CloseableIterator<?> i: iterators) {
			i.close();
		}
		if (registry != null) {
		    registry.deregisterResource(this);
		}
	}
}

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

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * This merging iterator merges multiple iterators into a single one.
 * 
 * TODO For queries across multiple nodes, merge asynchronously by running sub-iterators in 
 * different threads and merge the result as they arrive.
 * 
 * @author Tilmann Zaeschke
 *
 * @param <E> The element type
 */
public class MergingIterator<E> implements CloseableIterator<E> {

	private final List<CloseableIterator<E>> iterators = new LinkedList<CloseableIterator<E>>();
	private CloseableIterator<E> current;
	private final IteratorRegistry registry;
	private boolean isClosed;
	private final boolean failOnClosedQuery;
	
    public MergingIterator(boolean failOnClosedQuery) {
        this.registry = null;
        this.failOnClosedQuery = failOnClosedQuery;
    }

    public MergingIterator(IteratorRegistry registry, boolean failOnClosedQuery) {
        this.registry = registry;
        this.failOnClosedQuery = failOnClosedQuery;
        registry.registerResource(this);
    }

    @Override
	public boolean hasNext() {
    	if (isClosed) {
    		if (failOnClosedQuery) {
    			throw DBLogger.newUser("This iterator has been closed.");
    		} else {
    			return false;
    		}
    	}
		if (current == null) {
			return false;
		}
		
		while (!current.hasNext()) {
			if (iterators.isEmpty()) {
				current = null;
				return false;
			}
			current.close();
			current = iterators.remove(0);
		}
		return true;
	}

	@Override
	public E next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		
		return current.next();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	public void add(CloseableIterator<E> it) {
		if (current == null) {
			current = it;
		} else {
			iterators.add(it);
		}
	}

	@Override
	public void close() {
		isClosed = true;
		if (current != null) {
			current.close();
		}
		for (CloseableIterator<E> i: iterators) {
			i.close();
		}
		if (registry != null) {
		    registry.deregisterResource(this);
		}
	}	
}

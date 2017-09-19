/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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

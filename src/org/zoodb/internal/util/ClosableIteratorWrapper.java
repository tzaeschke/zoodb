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

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * This iterator wraps a non-closable iterator into a closable iterator.
 * 
 * @author Tilmann Zaeschke
 *
 * @param <E> The entry type
 */
public class ClosableIteratorWrapper<E> implements CloseableIterator<E> {

	private Iterator<E> current;
	private final IteratorRegistry registry;
	private boolean isClosed;
	private final boolean failOnClosedQuery;
	
	/**
	 * Constructor to construct empty iterators.
	 * @param failOnClosedQuery Whether to fast fail operations on closed queries
	 */
    public ClosableIteratorWrapper(boolean failOnClosedQuery) {
        this.registry = null;
        this.failOnClosedQuery = failOnClosedQuery;
    }

    public ClosableIteratorWrapper(Iterator<E> iter, 
    		IteratorRegistry registry, boolean failOnClosedQuery) {
        this.registry = registry;
        this.failOnClosedQuery = failOnClosedQuery;
        if (registry != null) {
        	registry.registerResource(this);
        }
        this.current = iter;
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
		
		if (!current.hasNext()) {
			current = null;
			return false;
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

	@Override
	public void close() {
		isClosed = true;
		if (registry != null) {
		    registry.deregisterResource(this);
		}
	}	
}

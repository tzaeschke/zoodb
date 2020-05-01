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
	private final CloseableResource parent;
	private boolean isClosed;
	private final boolean failOnClosedQuery;
	
	/**
	 * Constructor to construct empty iterators.
	 * @param failOnClosedQuery Whether to fast fail operations on closed queries
	 */
    public ClosableIteratorWrapper(boolean failOnClosedQuery) {
        this.parent = null;
        this.failOnClosedQuery = failOnClosedQuery;
    }

    public ClosableIteratorWrapper(Iterator<E> iter, 
    		CloseableResource parent, boolean failOnClosedQuery) {
        this.parent = parent;
        this.failOnClosedQuery = failOnClosedQuery;
        this.current = iter;
    }

    @Override
	public boolean hasNext() {
    	if (isClosed()) {
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

	private boolean isClosed() {
		return isClosed || (parent != null && parent.isClosed());
	}

	@Override
	public void close() {
		//This seems a bit pointless, but JDO determines that iterators,
		//for example on extends, are closeable.
		isClosed = true;
	}	
}

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
package org.zoodb.internal.query;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;

/**
 * This merging iterator merges multiple iterators into a single one.
 * 
 * @author Tilmann Zaeschke
 *
 * @param <E>
 */
public class QueryMergingIterator<E> implements Iterator<E> {

	private final LinkedList<Iterator<E>> iterators = new LinkedList<Iterator<E>>();
	private final LinkedList<Collection<E>> collections = new LinkedList<Collection<E>>();
	private Iterator<E> current;
	
    public QueryMergingIterator() {
    	//nothing
    }

    public QueryMergingIterator(Iterator<E> iter) {
    	iterators.add(iter);
    	current = iter;
    }

    @Override
	public boolean hasNext() {
		if (current == null) {
			return false;
		}
		
		while (!current.hasNext()) {
			if (iterators.isEmpty()) {
				if (collections.isEmpty()) {
					current = null;
					return false;
				}
				current = collections.remove(0).iterator();
				continue;
			}
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

	public void add(Iterator<E> it) {
		if (current == null) {
			current = it;
		} else {
			iterators.add(it);
		}
	}

	/**
	 * Adds a collection to the merged iterator. The iterator of the
	 * collection is only requested after other iterators are exhausted.
	 * This can help avoiding concurrent modification exceptions.
	 * 
	 * @param collection
	 */
	public void addColl(Collection<E> collection) {
		if (current == null) {
			current = collection.iterator();
		} else {
			collections.add(collection);
		}
	}
}

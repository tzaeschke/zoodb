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
 * @param <E> Iterator type
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
	 * @param collection The collection that should be added to the iterator.
	 */
	public void addColl(Collection<E> collection) {
		if (current == null) {
			current = collection.iterator();
		} else {
			collections.add(collection);
		}
	}
}

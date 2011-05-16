package org.zoodb.jdo.stuff;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * This merging iterator merges multiple iterators into a single one.
 * 
 * TODO For queries across multiple nodes, merge asynchronously by running sub-iterators in 
 * different threads and merge the result as they arrive.
 * 
 * @author Tilmann Zäschke
 *
 * @param <E>
 */
public class MergingIterator<E> implements Iterator<E> {

	private final List<Iterator<E>> iterators = new LinkedList<Iterator<E>>();
	private Iterator<E> current;
	
	@Override
	public boolean hasNext() {
		if (current == null) {
			return false;
		}
		
		while (!current.hasNext()) {
			if (iterators.isEmpty()) {
				current = null;
				return false;
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
		current.remove();
	}

	public void add(Iterator<E> it) {
		iterators.add(it);
		if (current == null) {
			current = it;
		}
	}
	
}

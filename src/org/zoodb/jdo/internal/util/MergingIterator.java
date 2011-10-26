package org.zoodb.jdo.internal.util;

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
public class MergingIterator<E> implements CloseableIterator<E> {

	private final List<CloseableIterator<E>> iterators = new LinkedList<CloseableIterator<E>>();
	private CloseableIterator<E> current;
	
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
		current.remove();
	}

	public void add(CloseableIterator<E> it) {
		iterators.add(it);
		if (current == null) {
			current = it;
		}
	}

	@Override
	public void close() {
		if (current != null) {
			current.close();
		}
		for (CloseableIterator<E> i: iterators) {
			i.close();
		}
	}
	
}

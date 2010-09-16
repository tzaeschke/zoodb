package org.zoodb.jdo;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class ExtentIterator<E> implements Iterator<E> {

	//_it==null means closed
	private Iterator<E> _it;
	
	ExtentIterator(Iterator<E> it) {
		_it = it;
	}
	
	@Override
	public boolean hasNext() {
		if (_it == null) {
			return false;
		}
		return _it.hasNext();
	}

	@Override
	public E next() {
		if (_it == null) {
			throw new NoSuchElementException("This iterator has been closed.");
		}
		return _it.next();
	}

	@Override
	public void remove() {
		if (_it == null) {
			throw new NoSuchElementException("This iterator has been closed.");
		}
		_it.remove();
	}

	public void close() {
		_it = null;
	}
}

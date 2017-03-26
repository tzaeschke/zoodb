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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.zoodb.internal.Session;

/**
 * Synchronized read-only collection.
 * 
 * @author ztilmann
 *
 */
public class SynchronizedROCollection<E> implements List<E>, Closeable {

	private Collection<E> c;
	private final ClientLock lock;
	private final Session session;
	private boolean isClosed = false;
	
	//TODO this is really bad and should happen on the server...
	private int minIncl;
	private int maxExcl;
	
	public SynchronizedROCollection(Collection<E> c, Session session, long minIncl, long maxExcl) {
		this.c = c;
		this.lock = session.getLock();
		this.minIncl = minIncl > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) minIncl;
		this.maxExcl = maxExcl > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) maxExcl;
		this.session = session;
		session.registerResource(this);
	}
	
	@Override
	public int size() {
		try {
			lock.lock();
			adjustSize();
			return c.size();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public boolean isEmpty() {
		try {
			lock.lock();
			adjustSize();
			return c.isEmpty();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public boolean contains(Object o) {
		// Not required, see JDO 3.2
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<E> iterator() {
		try {
			lock.lock();
    		boolean failOnClosedQuery = session.getConfig().getFailOnClosedQueries();
    		if (!session.isActive() && !session.getConfig().getNonTransactionalRead()) { 
	    		if (failOnClosedQuery) {
	    			//One of those will definitely fail
	    			session.checkOpen();
	    			session.checkActiveRead();
	    		} else {
	    			return new ClosableIteratorWrapper<>(failOnClosedQuery);
	    		}
	    	}
			ClosableIteratorWrapper<E> iter = 
					new ClosableIteratorWrapper<>(c.iterator(), session, failOnClosedQuery);
			return new SynchronizedROIterator<E>(iter, lock, minIncl, maxExcl);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Object[] toArray() {
		// Not required, see JDO 3.2
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		// Not required, see JDO 3.2
		throw new UnsupportedOperationException();
	}

	private void adjustSize() {
		if (isClosed && session.getConfig().getFailOnClosedQueries()) {
			//One of those will fail...
			session.checkOpen();
			session.checkActiveRead();
		}
		if (minIncl == 0 && maxExcl == Integer.MAX_VALUE) {
			//we can ignore this
			return;
		}
		//Call this as late as necessary, size() can be very expensive
		if (minIncl >= c.size()) {
			c = new ArrayList<>();
		} else {
			maxExcl = maxExcl > c.size() ? c.size() : maxExcl;
			if (!(c instanceof List)) {
				c = new ArrayList<>(c);
			}
			//TODO argh!!!!
			c = ((List)c).subList(minIncl, maxExcl);
		}
		minIncl = 0;
		maxExcl = Integer.MAX_VALUE;
	}
	
	@Override
	public boolean add(E e) {
		throw new UnsupportedOperationException("This list is unmodifiable.");
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException("This list is unmodifiable.");
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		// Not required, see JDO 3.2
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		throw new UnsupportedOperationException("This list is unmodifiable.");
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException("This list is unmodifiable.");
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException("This list is unmodifiable.");
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException("This list is unmodifiable.");
	}

	@Override
	public void close() throws IOException {
		c = Collections.emptyList();
		session.deregisterResource(this);
		isClosed = true;
	}

	@Override
	public boolean addAll(int index, Collection<? extends E> c) {
		throw new UnsupportedOperationException("This list is unmodifiable.");
	}

	@Override
	public E get(int index) {
		// Not required, see JDO 3.2
		throw new UnsupportedOperationException();
	}

	@Override
	public E set(int index, E element) {
		throw new UnsupportedOperationException("This list is unmodifiable.");
	}

	@Override
	public void add(int index, E element) {
		throw new UnsupportedOperationException("This list is unmodifiable.");
	}

	@Override
	public E remove(int index) {
		throw new UnsupportedOperationException("This list is unmodifiable.");
	}

	@Override
	public int indexOf(Object o) {
		// Not required, see JDO 3.2
		throw new UnsupportedOperationException();
	}

	@Override
	public int lastIndexOf(Object o) {
		//TODO
		//TODO
		//TODO
		//TODO Should we write in the Exception that we don't support this because of cursored results?
		//TODO
		//TODO
		//TODO
		// Not required, see JDO 3.2
		throw new UnsupportedOperationException();
	}

	@Override
	public ListIterator<E> listIterator() {
		// Not required, see JDO 3.2
		throw new UnsupportedOperationException();
	}

	@Override
	public ListIterator<E> listIterator(int index) {
		// Not required, see JDO 3.2
		throw new UnsupportedOperationException();
	}

	@Override
	public List<E> subList(int fromIndex, int toIndex) {
		// Not required, see JDO 3.2
		throw new UnsupportedOperationException("Please use a query with RANGE instead.");
	}

}

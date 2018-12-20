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

	public static int WARNING_THRESHOLD = 100;
	
	private static final String MODIFICATION_ERROR = "Query results are unmidifiable.";
	
	private Collection<E> c;
	private final ClientLock lock;
	private final Session session;
	private boolean isClosed = false;
	//This indicates whether the incoming collection is based on a fixed size list or
	//on an iterator. If it is an iterator, we only provide the most basic functions:
	//- single iterator()
	//- isEmpty()
	private List<E> fixSizeList;
	//THis indicates whether we can still attempt to create a fixed size list...
	private boolean isCreationOfFixSizeListAllowed = true;
	
	//TODO this is really bad and should happen on the server...
	private int minIncl;
	private int maxExcl;
	
	public SynchronizedROCollection(Collection<E> c, Session session, long minIncl, long maxExcl) {
		this.c = c;
		this.lock = session.getLock();
		this.minIncl = minIncl > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) minIncl;
		this.maxExcl = maxExcl > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) maxExcl;
		this.session = session;
		this.fixSizeList = (c instanceof ArrayList) && minIncl == 0 ? (ArrayList<E>) c : null;
		session.registerResource(this);
	}
	
	@Override
	public int size() {
		checkCursoredResult();
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
		checkCursoredResult();
		return fixSizeList.contains(o);
	}

	@Override
	public Iterator<E> iterator() {
		try {
			lock.lock();
			//We can't create a fix size list after iteration has begun
			isCreationOfFixSizeListAllowed = false;
    		boolean failOnClosedQuery = session.getConfig().getFailOnClosedQueries();
    		if (!session.isActive() && !session.getNonTransactionalRead()) { 
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
			return new SynchronizedROIterator<>(iter, lock, minIncl, maxExcl);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Object[] toArray() {
		checkCursoredResult();
		return fixSizeList.toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		checkCursoredResult();
		return fixSizeList.toArray(a);
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
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException(MODIFICATION_ERROR);
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		checkCursoredResult();
		return fixSizeList.containsAll(c);
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		throw new UnsupportedOperationException(MODIFICATION_ERROR);
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException(MODIFICATION_ERROR);
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException(MODIFICATION_ERROR);
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException(MODIFICATION_ERROR);
	}

	@Override
	public void close() throws IOException {
		c = Collections.emptyList();
		session.deregisterResource(this);
		isClosed = true;
	}

	@Override
	public boolean addAll(int index, Collection<? extends E> c) {
		throw new UnsupportedOperationException(MODIFICATION_ERROR);
	}

	@Override
	public E get(int index) {
		checkCursoredResult();
		return fixSizeList.get(index);
	}

	@Override
	public E set(int index, E element) {
		throw new UnsupportedOperationException(MODIFICATION_ERROR);
	}

	@Override
	public void add(int index, E element) {
		throw new UnsupportedOperationException(MODIFICATION_ERROR);
	}

	@Override
	public E remove(int index) {
		throw new UnsupportedOperationException(MODIFICATION_ERROR);
	}

	@Override
	public int indexOf(Object o) {
		checkCursoredResult();
		return fixSizeList.indexOf(o);
	}

	@Override
	public int lastIndexOf(Object o) {
		checkCursoredResult();
		return fixSizeList.lastIndexOf(o);
	}

	@Override
	public ListIterator<E> listIterator() {
		checkCursoredResult();
		return fixSizeList.listIterator();
	}

	@Override
	public ListIterator<E> listIterator(int index) {
		checkCursoredResult();
		return fixSizeList.listIterator(index);
	}

	@Override
	public List<E> subList(int fromIndex, int toIndex) {
		if (fixSizeList == null) {
			// Not required, see JDO 3.2
			throw new UnsupportedOperationException("Please use a query with RANGE instead.");
		} else {
			return fixSizeList.subList(fromIndex, toIndex);
		}
	}

	private void checkCursoredResult() {
		if (fixSizeList == null) {
			if (isCreationOfFixSizeListAllowed) {
				isCreationOfFixSizeListAllowed = false;
				fixSizeList = new ArrayList<>(c);
				if (fixSizeList.size() > WARNING_THRESHOLD) {
					Session.LOGGER.warn("This operation on a query result loaded {} object into memory. "
							+ "Avoid using function like size() if this is not desired", fixSizeList.size()); 
				}
				if (minIncl > fixSizeList.size()) {
					fixSizeList.clear();
					return;
				}
				if (minIncl > 0) {
					if (maxExcl < fixSizeList.size()) {
						fixSizeList = fixSizeList.subList(minIncl, maxExcl);
					} else {
						fixSizeList = fixSizeList.subList(minIncl, fixSizeList.size());
					}
				} else if (maxExcl < fixSizeList.size()) {
					fixSizeList = fixSizeList.subList(0, maxExcl);
				}
				return;
			}
			// Not required, see JDO 3.2
			throw new UnsupportedOperationException(
					"This operation is not supported in cursored query results.");
		}
	}
	
}

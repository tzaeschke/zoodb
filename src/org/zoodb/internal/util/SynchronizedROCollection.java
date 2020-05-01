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
public class SynchronizedROCollection<E> implements List<E>, CloseableResource {

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
					new ClosableIteratorWrapper<>(c.iterator(), this, failOnClosedQuery);
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

	@Override
	public boolean isClosed() {
		return isClosed;
	}

}

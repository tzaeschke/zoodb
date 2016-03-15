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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Synchronized read-only collection.
 * 
 * @author ztilmann
 *
 */
public class SynchronizedROCollection<E> implements Collection<E> {

	private Collection<E> c;
	private final ClientLock lock;
	
	//TODO this is really bad and should happen on the server...
	private int minIncl;
	private int maxExcl;
	
	public SynchronizedROCollection(Collection<E> c, ClientLock lock, long minIncl, long maxExcl) {
		this.c = c;
		this.lock = lock;
		this.minIncl = minIncl > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) minIncl;
		this.maxExcl = maxExcl > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) maxExcl;
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
		try {
			lock.lock();
			adjustSize();
			return c.contains(o);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Iterator<E> iterator() {
		try {
			lock.lock();
			return new SynchronizedROIterator<E>(c.iterator(), lock, minIncl, maxExcl);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Object[] toArray() {
		try {
			lock.lock();
			adjustSize();
			return c.toArray();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public <T> T[] toArray(T[] a) {
		try {
			lock.lock();
			adjustSize();
			return c.toArray(a);
		} finally {
			lock.unlock();
		}
	}

	private void adjustSize() {
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
		throw new UnsupportedOperationException("This collection is unmodifiable.");
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException("This collection is unmodifiable.");
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		try {
			lock.lock();
			return this.c.containsAll(c);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		throw new UnsupportedOperationException("This collection is unmodifiable.");
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException("This collection is unmodifiable.");
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException("This collection is unmodifiable.");
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException("This collection is unmodifiable.");
	}

}

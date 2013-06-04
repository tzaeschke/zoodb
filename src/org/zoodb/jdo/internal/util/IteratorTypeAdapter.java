/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.jdo.internal.util;



/**
 * This class does nothing else but turning an Iterator<? extends T> into Iterator<T>.
 * 
 * @author Tilmann Zaschke
 *
 * @param <T>
 */
public class IteratorTypeAdapter<T> implements CloseableIterator<T> {

	private final CloseableIterator<? extends T> iter;
	
	public IteratorTypeAdapter(CloseableIterator <? extends T> iter) {
		this.iter = iter;
	}
	
	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	@Override
	public T next() {
		return iter.next();
	}

	@Override
	public void remove() {
		iter.remove();
	}

	@Override
	public void close() {
		iter.close();
	}

	@Override
	public void refresh() {
		iter.refresh();
	}

}

/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;



public interface PrimLongMap<T> {

	public abstract T get(long keyBits);

	public abstract T put(long keyBits, T obj);

	public abstract T remove(long keyBits);

	public abstract int size();

	public abstract Collection<T> values();

	public abstract void clear();

	public abstract boolean containsKey(long keyBits);

	public abstract boolean containsValue(T value);

	public abstract void putAll(PrimLongMap<? extends T> map);

	public abstract Set<Long> keySet();

	public abstract Set<PrimLongEntry<T>> entrySet();

	public interface PLMValueIterator<T> extends Iterator<T> {

		boolean hasNextEntry();

		T nextValue();
		
	}

	public interface PrimLongEntry<T> {

		long getKey();

		T getValue();
		
	}
	
}

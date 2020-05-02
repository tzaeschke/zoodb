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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;



public interface PrimLongMap<T> {

	T get(long keyBits);

	T put(long keyBits, T obj);

	/**
	 * 
	 * @param keyBits The key
	 * @param obj The value
	 * @return The previous value or 'null' the the key did not exist.
	 * @see Map#putIfAbsent(Object, Object)
	 */
	T putIfAbsent(long keyBits, T obj);

	T remove(long keyBits);

	int size();

	Collection<T> values();

	void clear();

	boolean containsKey(long keyBits);

	boolean containsValue(T value);

	void putAll(PrimLongMap<? extends T> map);

	Set<Long> keySet();

	Set<PrimLongEntry<T>> entrySet();

	interface PLMValueIterator<T> extends Iterator<T> {

		boolean hasNextEntry();

		T nextValue();
		
	}

	interface PrimLongEntry<T> {

		long getKey();

		T getValue();
		
	}
	
}

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
package org.zoodb.jdo.impl;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class ExtentIterator<E> implements Iterator<E> {

	//_it==null means closed
	private Iterator<E> it;
	
	ExtentIterator(Iterator<E> it) {
		this.it = it;
	}
	
	@Override
	public boolean hasNext() {
		if (it == null) {
			return false;
		}
		return it.hasNext();
	}

	@Override
	public E next() {
		if (it == null) {
			throw new NoSuchElementException("This iterator has been closed.");
		}
		return it.next();
	}

	@Override
	public void remove() {
		if (it == null) {
			throw new NoSuchElementException("This iterator has been closed.");
		}
		it.remove();
	}

	public void close() {
		it = null;
	}
}

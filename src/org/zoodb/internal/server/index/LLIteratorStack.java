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
package org.zoodb.internal.server.index;

import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * Stack used by LLIterators.

 * @author Tilmann Zaeschke
 */
class LLIteratorStack {

	private LLIndexPage[] pages = new LLIndexPage[20];
	private short[] positions = new short[20];
	private int size = 0;

	void push(LLIndexPage page, short pos) {
		if (size >= pages.length) {
			pages = Arrays.copyOf(pages, pages.length * 2);
			positions = Arrays.copyOf(positions, positions.length * 2);
		}
		pages[size] = page;
		positions[size] = pos;
		size++;
	}

	void pop() {
		if (size <= 0) {
			throw new NoSuchElementException();
		}
		size--;
	}

	LLIndexPage currentPage() {
		return pages[size - 1];
	}

	short currentPos() {
		return positions[size - 1];
	}

	boolean isEmpty() {
		return size <= 0;
	}
}
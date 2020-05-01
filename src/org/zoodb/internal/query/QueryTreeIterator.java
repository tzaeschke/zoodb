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
package org.zoodb.internal.query;

import java.util.NoSuchElementException;

/**
 * QueryIteratorV4 class. It iterates over the terms of a query tree. Useful for evaluating
 * candidate objects with a query.
 * 
 * @author Tilmann Zaeschke
 */
public final class QueryTreeIterator {
	private QueryTreeNode currentNode;
	//private boolean askFirst = true;
	private final boolean[] askFirstA = new boolean[20]; //0==first; 1==2nd; 2==done
	private int askFirstD = 0; //depth: 0=root
	private QueryTerm nextElement = null;

	QueryTreeIterator(QueryTreeNode node) {
		currentNode = node;
		for (int i = 0; i < askFirstA.length; i++) {
			askFirstA[i] = true;
		}
		nextElement = findNext();
	}


	public boolean hasNext() {
		return (nextElement != null);
	}


	/**
	 * To avoid duplicate work in next() and hasNext() (both can locate the next element),
	 * we automatically move to the next element when the previous is returned. The hasNext()
	 * method then becomes trivial.
	 * @return next element.
	 */
	public QueryTerm next() {
		if (nextElement == null) {
			throw new NoSuchElementException();
		}
		QueryTerm t = nextElement;
		nextElement = findNext();
		return t;
	}

	/**
	 * Also, ASK nur setzen wenn ich hoch komme?
	 * runter: true-> first;  false second;
	 * hoch: true-> nextSecond; false-> nextUP
	 */
	private QueryTerm findNext() {
		//Walk down first branch
		if (askFirstA[askFirstD]) {
			while (currentNode.firstTerm() == null) {
				//remember that we already walked down the first branch
				askFirstD++;
				currentNode = currentNode.firstNode();
			}
			askFirstA[askFirstD] = false;
			return currentNode.firstTerm();
		} 

		//do we have a second branch?
		if (currentNode.isUnary()) {
			return findUpwards();
		}

		//walk down second branch
		if (currentNode.secondTerm() != null) {
			//dirty hack
			if (currentNode.secondTerm() != nextElement) {
				return currentNode.secondTerm();
			}
			//else: we have been here before!
			//walk back up
			return findUpwards();
		} else {
			currentNode = currentNode.secondNode();
			askFirstD++;
			return findNext();
		}
	}

	private QueryTerm findUpwards() {
		if (currentNode.p == null) {
			return null;
		}

		do {
			//clean up behind me before moving back up
			askFirstA[askFirstD] = true;
			askFirstD--;
			currentNode = currentNode.parent();
		} while (currentNode.p != null && (currentNode.isUnary() || !askFirstA[askFirstD]));

		//remove, only for DEBUG
		//            if (_currentNode == null) {
		//                throw new NoSuchElementException();
		//            }
		//if 'false' then we are finished 
		if (!askFirstA[askFirstD]) {
			return null;
		}
		//indicate that we want the second branch now
		askFirstA[askFirstD] = false;
		//walk down second branch
		return findNext();
	}
}
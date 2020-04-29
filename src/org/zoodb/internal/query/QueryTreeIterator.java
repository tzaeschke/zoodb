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
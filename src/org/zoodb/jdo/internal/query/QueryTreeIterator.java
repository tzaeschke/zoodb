package org.zoodb.jdo.internal.query;

import java.util.NoSuchElementException;

/**
 * QueryIterator class. It iterates over the terms of a query tree. Useful for evaluating
 * candidate objects with a query.
 * 
 * @author Tilmann Zaeschke
 */
public final class QueryTreeIterator {
	private QueryTreeNode _currentNode;
	//private boolean askFirst = true;
	private final boolean[] askFirstA = new boolean[20]; //0==first; 1==2nd; 2==done
	private int askFirstD = 0; //depth: 0=root
	private QueryTerm nextElement = null;

	QueryTreeIterator(QueryTreeNode node) {
		_currentNode = node;
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
			while (_currentNode.firstTerm() == null) {
				//remember that we already walked down the first branch
				askFirstD++;
				_currentNode = _currentNode.firstNode();
			}
			askFirstA[askFirstD] = false;
			return _currentNode.firstTerm();
		} 

		//do we have a second branch?
		if (_currentNode.isUnary()) {
			return findUpwards();
		}

		//walk down second branch
		if (_currentNode.secondTerm() != null) {
			//dirty hack
			if (_currentNode.secondTerm() != nextElement) {
				return _currentNode.secondTerm();
			}
			//else: we have been here before!
			//walk back up
			return findUpwards();
		} else {
			_currentNode = _currentNode.secondNode();
			askFirstD++;
			return findNext();
		}
	}

	private QueryTerm findUpwards() {
		if (_currentNode._p == null) {
			return null;
		}

		do {
			//clean up behind me before moving back up
			askFirstA[askFirstD] = true;
			askFirstD--;
			_currentNode = _currentNode.parent();
		} while (_currentNode._p != null && (_currentNode.isUnary() || !askFirstA[askFirstD]));

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
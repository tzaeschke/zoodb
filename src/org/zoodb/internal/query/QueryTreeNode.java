/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
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

import java.util.List;

import org.zoodb.internal.DataDeSerializerNoClass;
import org.zoodb.internal.query.QueryParser.LOG_OP;
import org.zoodb.internal.util.DBLogger;

/**
 * A node of the query tree. Each node has an operator. The input for the operators can be
 * other nodes (sub-nodes) or QueryTerms.
 * 
 * @author Tilmann Zaeschke
 */
public final class QueryTreeNode {
	
	QueryTreeNode _n1;
	QueryTreeNode _n2;
	QueryTerm _t1;
	QueryTerm _t2;
	LOG_OP _op;
	QueryTreeNode _p;
	
	/** tell whether there is more than one child attached.
	    root nodes and !() node have only one child. */
	boolean isUnary() {
		return (_n2==null) && (_t2==null);
	}
	
	private boolean isBranchIndexed() {
		if (_t1 != null && _t1.getFieldDef().isIndexed()) {
			return true;
		}
		if (_t2 != null && _t2.getFieldDef().isIndexed()) {
			return true;
		}
		if (_n1 != null && _n1.isBranchIndexed()) {
			return true;
		}
		if (_n2 != null && _n2.isBranchIndexed()) {
			return true;
		}
		return false;
	}


	QueryTreeNode(QueryTreeNode n1, QueryTerm t1, LOG_OP op, QueryTreeNode n2, QueryTerm t2, 
			boolean negate) {
		_n1 = n1;
		_t1 = t1;
		_n2 = n2;
		_t2 = t2;
		if (op != null) {
			_op = op.inverstIfTrue(negate);
		}
		relateToChildren();
	}

	QueryTreeNode relateToChildren() {
		if (_n1 != null) {
			_n1._p = this;
		}
		if (_n2 != null) {
			_n2._p = this;
		}
		return this;
	}
	
	QueryTerm firstTerm() {
		return _t1;
	}

	QueryTreeNode firstNode() {
		return _n1;
	}

	QueryTerm secondTerm() {
		return _t2;
	}

	QueryTreeNode secondNode() {
		return _n2;
	}

	QueryTreeNode parent() {
		return _p;
	}

	QueryTreeNode root() {
		return _p == null ? this : _p.root();
	}

	public QueryTreeIterator termIterator() {
		if (_p != null) {
			throw DBLogger.newFatalInternal("Can not get iterator of child elements.");
		}
		return new QueryTreeIterator(this);
	}

	public boolean evaluate(Object o) {
		boolean first = (_n1 != null ? _n1.evaluate(o) : _t1.evaluate(o));
		//do we have a second part?
		if (_op == null) {
			return first;
		}
		if ( !first && _op == LOG_OP.AND) {
			return false;
		}
		if ( first && _op == LOG_OP.OR) {
			return true;
		}
		return (_n2 != null ? _n2.evaluate(o) : _t2.evaluate(o));
	}
	
	/**
	 * Evaluate the query directly on a byte buffer rather than on materialized objects. 
	 * @param pos
	 * @return Whether the object is a match.
	 */
	public boolean evaluate(DataDeSerializerNoClass dds, long pos) {
		boolean first = (_n1 != null ? _n1.evaluate(dds, pos) : _t1.evaluate(dds, pos));
		//do we have a second part?
		if (_op == null) {
			return first;
		}
		if ( !first && _op == LOG_OP.AND) {
			return false;
		}
		if ( first && _op == LOG_OP.OR) {
			return true;
		}
		return (_n2 != null ? _n2.evaluate(dds, pos) : _t2.evaluate(dds, pos));
	}
	
	/**
	 * This method splits a query into multiple queries for every occurrence of OR.
	 * It walks down the query tree recursively, always doubling the tree when encountering
	 * an OR in an indexed branch. A branch is 'indexed' if one of it's terms references
	 * an indexed field.
	 * 
	 * This method may introduce singular nodes (with one term only) that should be removed
	 * afterwards.
	 * 
	 * @param subQueries container for sub query candidates, which upon return
	 * contains one sub-query for every call.
	 */
	public void createSubs(List<QueryTreeNode> subQueries) {
		if (!isBranchIndexed()) {
			//nothing to do, stop searching this branch
			return;
		}
		
		//If op=OR and if and sub-nodes are indexed, then we split.
		if (LOG_OP.OR.equals(_op)) {
			//clone both branches (WHY ?)
			QueryTreeNode n1;
			if (_n1 != null) {
				n1 = _n1;
			} else {
				_n1 = n1 = new QueryTreeNode(null, _t1, null, null, null, false);
				_t1 = null;
			}
			QueryTreeNode n2;
			if (_n2 != null) {
				n2 = _n2.cloneBranch();
			} else {
				_n2 = n2 = new QueryTreeNode(null, _t2, null, null, null, false);
				_t2 = null;
			}
			//we remove the OR from the tree and assign the first clone/branch to any parent
			QueryTreeNode newTree;
			if (_p != null) {
				//remove local OR and replace with n1
				if (_p._n1 == this) {
					_p._n1 = n1;
					_p._t1 = null;
					_p.relateToChildren();
				} else if (_p._n2 == this) {
					_p._n2 = n1;
					_p._t2 = null;
					_p.relateToChildren();
				} else {
					throw new IllegalStateException();
				}
				//clone and replace with child number n2/t2
				//newTree = cloneSingle(n2, t2, null, null);
			} else {
				//no parent.
				//still remove this one and replace it with the first sub-node
				//TODO should we use a set for faster removal?
				subQueries.remove(this);
				subQueries.add(n1);
				if (n1 != null) {
					n1._p = null;
				}
				if (n2 != null) {
					n2._p = null;
				}
			}
			
			//now treat second branch and create a new parent for it, if necessary.
			if (_p != null) {
				newTree = _p.cloneTrunk(n1, n2);
			} else {
				newTree = n2;
			}
			//subQueriesCandidates.add(newTree.root());
			newTree.createSubs(subQueries);
			subQueries.add(newTree.root());
		}
		
		//go into sub-nodes
		if (_n1 != null) {
			_n1.createSubs(subQueries);
		}
		if (_n2 != null) {
			_n2.createSubs(subQueries);
		}
	}
	
	private QueryTreeNode cloneSingle(QueryTreeNode n1, QueryTerm t1, QueryTreeNode n2,
			QueryTerm t2) {
		QueryTreeNode ret = new QueryTreeNode(n1, t1, _op, n2, t2, false).relateToChildren();
		return ret;
	}
	
	/**
	 * Clones a tree upwards to the root, except for the branch that starts with 'stop', which is
	 * replaced by 'stopClone'.
	 * @param stop
	 * @param stopClone
	 * @return
	 */
	private QueryTreeNode cloneTrunk(QueryTreeNode stop, QueryTreeNode stopClone) {
		QueryTreeNode n1 = null;
		if (_n1 != null) {
			n1 = (_n1 == stop ? stopClone : _n1.cloneBranch());
		}
		QueryTreeNode n2 = null;
		if (_n2 != null) {
			n2 = _n2 == stop ? stopClone : _n2.cloneBranch();
		}
		
		QueryTreeNode ret = cloneSingle(n1, _t1, n2, _t2);
		if (_p != null) {
			_p.cloneTrunk(this, ret);
		}
		ret.relateToChildren();
		return ret;
	}
	
	private QueryTreeNode cloneBranch() {
		QueryTreeNode n1 = null;
		if (_n1 != null) {
			n1 = _n1.cloneBranch();
		}
		QueryTreeNode n2 = null;
		if (_n2 != null) {
			n2 = _n2.cloneBranch();
		}
		return cloneSingle(n1, _t1, n2, _t2);
	}

	public String print() {
		StringBuilder sb = new StringBuilder();
		if (_p == null) sb.append("#");
		sb.append("(");
		if (_n1 != null) {
			sb.append(_n1.print());
		} else {
			sb.append(_t1.print());
		}
		if (!isUnary()) {
			sb.append(" ");
			sb.append(_op);
			sb.append(" ");
			if (_n2 != null) {
				sb.append(_n2.print());
			} else {
				sb.append(_t2.print());
			}
		}
		sb.append(")");
		return sb.toString();
	}
}
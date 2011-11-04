/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.jdo.internal.query;

import java.util.List;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.jdo.internal.query.QueryParser.LOG_OP;

/**
 * A node of the query tree. Each node has an operator. The input for the operators can be
 * other nodes (sub-nodes) or QueryTerms.
 * 
 * @author Tilmann Zäschke
 */
public final class QueryTreeNode {
	
	private static enum BRANCH_TYPE {
		UNKNOWN,
		INDEXED,
		NOT_INDEXED;
	}
	
	
	
	QueryTreeNode _n1;
	QueryTreeNode _n2;
	QueryTerm _t1;
	QueryTerm _t2;
	LOG_OP _op;
	QueryTreeNode _p;
	
	private BRANCH_TYPE branchType = BRANCH_TYPE.UNKNOWN;
	
	/** tell whether there is more than one child attached.
	    root nodes and !() node have only one child. */
	boolean isUnary() {
		return (_n2==null) && (_t2==null);
	}
	
	private BRANCH_TYPE getBranchType() {
		if (branchType == BRANCH_TYPE.UNKNOWN) {
			if (_n1 != null) {
				branchType = _n1.getBranchType();
			} else {
				branchType = 
					_t1.getFieldDef().isIndexed() ? BRANCH_TYPE.INDEXED : BRANCH_TYPE.NOT_INDEXED;
			}
		}
		if (branchType == BRANCH_TYPE.UNKNOWN && !isUnary()) {
			if (_n2 != null) {
				branchType = _n2.getBranchType();
			} else {
				branchType = 
					_t2.getFieldDef().isIndexed() ? BRANCH_TYPE.INDEXED : BRANCH_TYPE.NOT_INDEXED;
			}
		}
		
		return branchType;
	}

	QueryTreeNode(QueryTreeNode n1, QueryTerm t1, LOG_OP op, QueryTreeNode n2, QueryTerm t2) {
		_n1 = n1;
		_t1 = t1;
		_n2 = n2;
		_t2 = t2;
		_op = op;
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
			throw new JDOFatalDataStoreException("Can not get iterator of child elements.");
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

	public QueryTreeNode createSubs(List<QueryTreeNode> subQueriesCandidates) {
		if (LOG_OP.OR.equals(_op) && getBranchType() == BRANCH_TYPE.INDEXED) {
			//clone both branches (WHY ?)
			QueryTreeNode n1;
			QueryTerm t1;
			if (_n1 != null) {
				n1 = _n1.cloneBranch();
				t1 = null;
			} else {
				n1 = null;
				t1 = _t1;
			}
			QueryTreeNode n2;
			QueryTerm t2;
			if (_n2 != null) {
				n2 = _n2.cloneBranch();
				t2 = null;
			} else {
				n2 = null;
				t2 = _t2;
			}
			//we remove the OR from the tree
			QueryTreeNode newTree;
			if (_p != null) {
				//remove local OR and replace with n1/t1
				if (_p._n1 == this) {
					_p._n1 = n1;
					_p._t1 = t1;
					_p.relateToChildren();
				} else if (_p._n2 == this) {
					_p._n2 = n1;
					_p._t2 = t1;
					_p.relateToChildren();
				} else {
					//TODO remove
					throw new IllegalStateException();
				}
				//clone and replace with child number n2/t2
				//newTree = cloneSingle(n2, t2, null, null);
			} else {
				//no parent.
				//still remove this one
				if (n1 != null) {
					n1._p = null;
				}
				if (n2 != null) {
					n2._p = null;
				}
			}
			
			//TODO merge with if statements above
			//now treat second branch
			if (_p == null) {
				newTree = new QueryTreeNode(n2, t2, null, null, null).relateToChildren();
			} else {
				newTree = _p.cloneTrunk(this, n2);  //TODO term should also be 0!
				if (n2 == null) {
					if (newTree._t1 == null) {
						newTree._t1 = t2; 
					} else {
						newTree._t2 = t2; 
					}
				}
			}
			subQueriesCandidates.add(newTree.root());
		}
		
		//go into sub-nodes
		if (_n1 != null) {
			_n1.createSubs(subQueriesCandidates);
		}
		if (_n2 != null) {
			_n2.createSubs(subQueriesCandidates);
		}
		//only required for top level return
		return this;
	}
	
	private QueryTreeNode cloneSingle(QueryTreeNode n1, QueryTerm t1, QueryTreeNode n2,
			QueryTerm t2) {
		QueryTreeNode ret = new QueryTreeNode(n1, t1, _op, n2, t2).relateToChildren();
		return ret;
	}
	
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
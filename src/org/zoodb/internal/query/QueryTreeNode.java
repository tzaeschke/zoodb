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
	
	QueryTreeNode n1;
	QueryTreeNode n2;
	QueryTerm t1;
	QueryTerm t2;
	LOG_OP op;
	QueryTreeNode p;
	
	/** tell whether there is more than one child attached.
	    root nodes and !() node have only one child. */
	boolean isUnary() {
		return (n2==null) && (t2==null);
	}
	
	private boolean isBranchIndexed() {
		if (t1 != null && t1.getLhsFieldDef() != null && t1.getLhsFieldDef().isIndexed()) {
			return true;
		}
		if (t2 != null && t2.getLhsFieldDef() != null && t2.getLhsFieldDef().isIndexed()) {
			return true;
		}
		if (n1 != null && n1.isBranchIndexed()) {
			return true;
		}
		if (n2 != null && n2.isBranchIndexed()) {
			return true;
		}
		return false;
	}


	QueryTreeNode(QueryTreeNode n1, QueryTerm t1, LOG_OP op, QueryTreeNode n2, QueryTerm t2, 
			boolean negate) {
		this.n1 = n1;
		this.t1 = t1;
		this.n2 = n2;
		this.t2 = t2;
		if (op != null) {
			this.op = op.inverstIfTrue(negate);
		}
		relateToChildren();
	}

	QueryTreeNode relateToChildren() {
		if (n1 != null) {
			n1.p = this;
		}
		if (n2 != null) {
			n2.p = this;
		}
		return this;
	}
	
	QueryTerm firstTerm() {
		return t1;
	}

	QueryTreeNode firstNode() {
		return n1;
	}

	QueryTerm secondTerm() {
		return t2;
	}

	QueryTreeNode secondNode() {
		return n2;
	}

	QueryTreeNode parent() {
		return p;
	}

	QueryTreeNode root() {
		return p == null ? this : p.root();
	}

	public QueryTreeIterator termIterator() {
		if (p != null) {
			throw DBLogger.newFatalInternal("Cannot get iterator of child elements.");
		}
		return new QueryTreeIterator(this);
	}

	public boolean evaluate(Object o, Object[] params) {
		boolean first = (n1 != null ? n1.evaluate(o, params) : t1.evaluate(o, params));
		//do we have a second part?
		if (op == null) {
			return first;
		}
		if ( !first && op == LOG_OP.AND) {
			return false;
		}
		if ( first && op == LOG_OP.OR) {
			return true;
		}
		return (n2 != null ? n2.evaluate(o, params) : t2.evaluate(o, params));
	}
	
	/**
	 * Evaluate the query directly on a byte buffer rather than on materialized objects. 
	 * @param pos position in byte buffer
	 * @param dds DataDeSerializer
	 * @param params Query execution parameters
	 * @return Whether the object is a match.
	 */
	public boolean evaluate(DataDeSerializerNoClass dds, long pos, Object[] params) {
		boolean first = (n1 != null ? n1.evaluate(dds, pos, params) : t1.evaluate(dds, pos, params));
		//do we have a second part?
		if (op == null) {
			return first;
		}
		if ( !first && op == LOG_OP.AND) {
			return false;
		}
		if ( first && op == LOG_OP.OR) {
			return true;
		}
		return (n2 != null ? n2.evaluate(dds, pos, params) : t2.evaluate(dds, pos, params));
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
		if (LOG_OP.OR.equals(op)) {
			//clone both branches (WHY ?)
			QueryTreeNode node1;
			if (n1 != null) {
				node1 = n1;
			} else {
				n1 = node1 = new QueryTreeNode(null, t1, null, null, null, false);
				t1 = null;
			}
			QueryTreeNode node2;
			if (n2 != null) {
				node2 = n2.cloneBranch();
			} else {
				n2 = node2 = new QueryTreeNode(null, t2, null, null, null, false);
				t2 = null;
			}
			//we remove the OR from the tree and assign the first clone/branch to any parent
			QueryTreeNode newTree;
			if (p != null) {
				//remove local OR and replace with n1
				if (p.n1 == this) {
					p.n1 = node1;
					p.t1 = null;
					p.relateToChildren();
				} else if (p.n2 == this) {
					p.n2 = node1;
					p.t2 = null;
					p.relateToChildren();
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
				subQueries.add(node1);
				if (node1 != null) {
					node1.p = null;
				}
				if (node2 != null) {
					node2.p = null;
				}
			}
			
			//now treat second branch and create a new parent for it, if necessary.
			if (p != null) {
				newTree = p.cloneTrunk(node1, node2);
			} else {
				newTree = node2;
			}
			//subQueriesCandidates.add(newTree.root());
			newTree.createSubs(subQueries);
			subQueries.add(newTree.root());
		}
		
		//go into sub-nodes
		if (n1 != null) {
			n1.createSubs(subQueries);
		}
		if (n2 != null) {
			n2.createSubs(subQueries);
		}
	}
	
	private QueryTreeNode cloneSingle(QueryTreeNode n1, QueryTerm t1, QueryTreeNode n2,
			QueryTerm t2) {
		QueryTreeNode ret = new QueryTreeNode(n1, t1, op, n2, t2, false).relateToChildren();
		return ret;
	}
	
	/**
	 * Clones a tree upwards to the root, except for the branch that starts with 'stop', which is
	 * replaced by 'stopClone'.
	 * @param stop
	 * @param stopClone
	 * @return A cloned branch of the query tree
	 */
	private QueryTreeNode cloneTrunk(QueryTreeNode stop, QueryTreeNode stopClone) {
		QueryTreeNode node1 = null;
		if (n1 != null) {
			node1 = (n1 == stop ? stopClone : n1.cloneBranch());
		}
		QueryTreeNode node2 = null;
		if (n2 != null) {
			node2 = n2 == stop ? stopClone : n2.cloneBranch();
		}
		
		QueryTreeNode ret = cloneSingle(node1, t1, node2, t2);
		if (p != null) {
			p.cloneTrunk(this, ret);
		}
		ret.relateToChildren();
		return ret;
	}
	
	private QueryTreeNode cloneBranch() {
		QueryTreeNode node1 = null;
		if (n1 != null) {
			node1 = n1.cloneBranch();
		}
		QueryTreeNode node2 = null;
		if (n2 != null) {
			node2 = n2.cloneBranch();
		}
		return cloneSingle(node1, t1, node2, t2);
	}

	public String print() {
		StringBuilder sb = new StringBuilder();
		if (p == null) sb.append("#");
		sb.append("(");
		if (n1 != null) {
			sb.append(n1.print());
		} else {
			sb.append(t1.print());
		}
		if (!isUnary()) {
			sb.append(" ");
			sb.append(op);
			sb.append(" ");
			if (n2 != null) {
				sb.append(n2.print());
			} else {
				sb.append(t2.print());
			}
		}
		sb.append(")");
		return sb.toString();
	}
}
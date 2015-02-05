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

import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.server.index.BitTools;

public class QueryOptimizer {
	
	private final ZooClassDef clsDef;
	
	public QueryOptimizer(ZooClassDef clsDef) {
		this.clsDef = clsDef;
	}
	
	/**
	 * Determine index to use.
	 * 
	 * Policy:
	 * 1) Check if index are available. If not, do not perform any further query analysis (for now)
	 *    -> Query rewriting may still be able to optimize really stupid queries.
	 * 2) Create sub-queries
	 * 3) Analyse sub-queries to determine best index to use. Result may imply that index usage is
	 *    pointless (whole index range required). This could also be if one sub-query does not use
	 *    any index, in which case using an index for the rest slightly increases disk access 
	 *    (index read) but reduces CPU needs (only sub-query to process, not whole query).
	 * 4a) For each sub-query, determine index with smallest range/density.
	 * 4b) Check for required sorting. Using an according index can be of advantage, even if range 
	 *    is larger.
	 * 5) Merge queries with same index and overlapping ranges
	 * 6) merge results
	 * 
	 * @param queryTree
	 * @return Index to use.
	 */
	public List<QueryAdvice> determineIndexToUse(QueryTreeNode queryTree) {
		List<QueryAdvice> advices = new LinkedList<QueryAdvice>();
		List<ZooFieldDef> availableIndices = new LinkedList<ZooFieldDef>();
		for (ZooFieldDef f: clsDef.getAllFields()) {
			if (f.isIndexed()) {
				availableIndices.add(f);
			}
		}

		// step 1
		if (availableIndices.isEmpty()) {
			//no index usage
			advices.add( new QueryAdvice(queryTree) );
			return advices;
		}
		
		//step 2 - sub-queries
		//We split the query tree at every OR into sub queries, such that every sub-query contains
		//the full query but only one side of every OR. All ORs are removed.
		//-> Optimization: We remove only (and split only at) ORs where at least on branch
		//   uses an index. TODO
		List<QueryTreeNode> subQueries = new LinkedList<QueryTreeNode>();
		subQueries.add(queryTree);
		queryTree.createSubs(subQueries);
		
//		System.out.println("Query2: " + queryTree.print());
		for (QueryTreeNode sq: subQueries) {
			optimize(sq);
//			System.out.println("Sub-query: " + sq.print());
		}
		//TODO filter out terms that cannot become true.
		//if none is left, return empty set.
		
		IdentityHashMap<ZooFieldDef, Long> minMap = new IdentityHashMap<ZooFieldDef, Long>();
		IdentityHashMap<ZooFieldDef, Long> maxMap = new IdentityHashMap<ZooFieldDef, Long>();
		for (QueryTreeNode sq: subQueries) {
			advices.add(determineIndexToUseSub(sq, minMap, maxMap));
			minMap.clear();
			maxMap.clear();
		}
		
		//TODO merge queries
		//E.g.:
		// - if none uses an index (or at least one doesn't), return only the full query
		// - if ranges overlap, try to merge?

		//TODO optimisation: merge queries
		//for example the following query returns two identical sub-queries:
		//"_int == 123 || _int == 123" --> This is bad and should be avoided.
				
		//check for show-stoppers
		//-> in their case, we simply run the un-split query on the full type extent.
		for (QueryAdvice qa: advices) {
			//assuming that the term is not an empty term (contradicting sub-terms)
			if (qa == null) {
				//ah, one of them iterates over the whole result set.
				advices.clear();
				advices.add(qa);
				return advices;
			}
			//TODO instead of fixed values, use min/max of index.
			if (qa.getMin() <= Long.MIN_VALUE && qa.getMax() >= Long.MAX_VALUE) {
				//ah, one of them iterates over the whole result set.
				advices.clear();
				advices.add(qa);
				return advices;
			}
		}
		
		//check for overlapping / global min/max
		mergeAdvices(advices);
		
		return advices;
	}
	
	private static class AdviceComparator implements Comparator<QueryAdvice> {
		@Override
		public int compare(QueryAdvice o1, QueryAdvice o2) {
			if (o1.getMin() < o2.getMin()) {
				return -1;
			} else if(o1.getMin() > o2.getMin()) {
				return 1;
			} else {
				if (o1.getMax() < o2.getMax()) {
					return -1;
				} else if(o1.getMax() > o2.getMax()) {
					return 1;
				} else {
					return 0;
				}
			}
		}
	}
	
	
	private void mergeAdvices(List<QueryAdvice> advices) {
		//if they overlap, we should merge them to void duplicate loading effort and results.
		//if they don't overlap, we don't have to care about either.
		//-> assuming they all use the same index...
		if (advices.size() < 2) {
			//shortcut
			return;
		}
		IdentityHashMap<ZooFieldDef, TreeSet<QueryAdvice>> map = 
				new IdentityHashMap<ZooFieldDef, TreeSet<QueryAdvice>>();
		//sort QAs by index and by minValue
		for (QueryAdvice qa: advices) {
			TreeSet<QueryAdvice> subList = map.get(qa.getIndex());
			if (subList == null) {
				subList = new TreeSet<QueryAdvice>(new AdviceComparator());
				map.put(qa.getIndex(), subList);
			}
			subList.add(qa);
		}

		//merge
		boolean merged = false;
		for (QueryAdvice qa: advices) {
			TreeSet<QueryAdvice> subList = map.get(qa.getIndex());
			Iterator<QueryAdvice> iter = subList.iterator();
			QueryAdvice prev = iter.next();
			while (iter.hasNext()) {
				QueryAdvice current = iter.next();
				if (prev.getMax() >= current.getMin()) {
					prev.setMax(current.getMax());
					iter.remove();
					merged = true;
				} else {			
					prev = current;
				}
			}
		}

		if (merged) {
			advices.clear();
			for (TreeSet<QueryAdvice> subList: map.values()) {
				advices.addAll(subList);
			}
		}
	}

	/**
	 * 
	 * @param queryTree This is a sub-query that does not contain OR operands.
	 * @param maxMap2 
	 * @param minMap2 
	 * @return QueryAdvise
	 */
	private QueryAdvice determineIndexToUseSub(QueryTreeNode queryTree, 
			IdentityHashMap<ZooFieldDef, Long> minMap, 
			IdentityHashMap<ZooFieldDef, Long> maxMap) {
		//TODO determine the Lists directly by assigning ZooFields to term during parsing?
		QueryTreeIterator iter = queryTree.termIterator();
		while (iter.hasNext()) {
			QueryTerm term = iter.next();
			if (!term.isRhsFixed()) {
				//ignore terms with variable rhs
				continue;
			}
			ZooFieldDef f = term.getLhsFieldDef();
			if (!f.isIndexed()) {
				//ignore fields that are not index
				continue;
			}
			
			Long minVal = minMap.get(f);
			if (minVal == null) {
				//needs initialization
				minMap.put(f, f.getMinValue());
				maxMap.put(f, f.getMaxValue());
			}
			
			Long value;
            if (term.getValue(null) == QueryParser.NULL) {
                //ignoring null values. TODO is this correct?
                continue;
            } else if (term.getValue(null) instanceof Double) {
				value = BitTools.toSortableLong((Double)term.getValue(null));
            } else if (term.getValue(null) instanceof Float) {
				value = BitTools.toSortableLong((Float)term.getValue(null));
            } else if (term.getValue(null) instanceof Number) {
				value = ((Number)term.getValue(null)).longValue();
			} else if (term.getValue(null) instanceof String) {
				value = BitTools.toSortableLong((String) term.getValue(null));
			} else if (term.getValue(null) instanceof Boolean) {
				//pointless..., well pretty much, unless someone uses this to distinguish
				//very few 'true' from many 'false' or vice versa.
				continue;
			} else {
				throw new IllegalArgumentException("Type: " + term.getValue(null).getClass());
			}
			
			switch (term.getOp()) {
			case EQ: {
				//TODO check range and exit if EQ does not fit in remaining range
				minMap.put(f, value);
				maxMap.put(f, value);
				break;
			}
			case L:
				if (value < maxMap.get(f)) {
					maxMap.put(f, value - 1); //TODO does this work with floats?
				}
				break;
			case LE: 				
				if (value < maxMap.get(f)) {
					maxMap.put(f, value);
				}
				break;
			case A: 
				if (value > minMap.get(f)) {
					minMap.put(f, value + 1); //TODO does this work with floats?
				}
				break;
			case AE:
				if (value > minMap.get(f)) {
					minMap.put(f, value);
				}
				break;
			case NE:
			case STR_matches:
			case STR_contains_NON_JDO:
			case STR_endsWith:
				//ignore
				break;
			case STR_startsWith:
				String prefix = (String) term.getValue(null);
				long keyMin = BitTools.toSortableLongPrefixMinHash(prefix);
				long keyMax = BitTools.toSortableLongPrefixMinHash(prefix);
				if (keyMin > minMap.get(f)) {
					minMap.put(f, keyMin);
				}
				if (keyMax > maxMap.get(f)) {
					maxMap.put(f, keyMax);
				}
				break;
			default: 
				throw new IllegalArgumentException("Name: " + term.getOp());
			}
			
			//TODO take into accoutn not-operators (x>1 && x<10) && !(x>5 && X <6) ??
			// -> Hopefully this optimization is marginal and negligible.
			//But it may break everything!
		}
		
		if (minMap.isEmpty()) {
			//return default query
			return new QueryAdvice(queryTree);
		}
		
		//the advised index to use...
		// start with first
		ZooFieldDef def = minMap.keySet().iterator().next();
		QueryAdvice qa = new QueryAdvice(queryTree);
		qa.setIndex( def );
		qa.setMin( minMap.get(def) );
		qa.setMax( maxMap.get(def) );
		
		//only one index left? -> Easy!!!
		//TODO well, better not use it if it covers the whole range? Maybe for sorting?
		if (minMap.size() == 1) {
			qa.setIndex( minMap.keySet().iterator().next() );
			return qa;
		}
		
		for (Map.Entry<ZooFieldDef, Long> me2: minMap.entrySet()) {
			long min2 = me2.getValue();
			long max2 = maxMap.get(me2.getKey());
			//TODO fix for very large values
			if ((max2-min2) < (qa.getMax() - qa.getMin())) {
				qa.setIndex( me2.getKey() );
				qa.setMin( min2 );
				qa.setMax( max2 );
			}
		}
		
		if (qa.getIndex().isString()) {
			//For String we have to extend the range because of the trailing hashcode
			qa.setMin(BitTools.getMinPosInPage(qa.getMin()));
			qa.setMax(BitTools.getMaxPosInPage(qa.getMax()));
		}

//		DatabaseLogger.debugPrintln(0, "Using index: " + def.getName());
		return qa;
	}

	private void optimize(QueryTreeNode q) {
		stripUnaryNodes(q);
	}

	private void stripUnaryNodes(QueryTreeNode q) {
		while (q.isUnary() && q._n1 != null) {
			//this is a unary root node that shouldn't be one
			q._op = q._n1._op;
			q._n2 = q._n1._n2;
			q._t2 = q._n1._t2;
			q._t1 = q._n1._t1;
			q._n1 = q._n1._n1;
			q.relateToChildren();
		}
		//check unary nodes if they are not root / pull down leaf-unaries
		if (q.isUnary() && q._p != null) {
			if (q._p._n1 == q) {
				q._p._n1 = q._n1;
				q._p._t1 = q._t1;
				if (q._n1 != null) {
					q._n1._p = q._p;
				}
			} else {
				q._p._n2 = q._n1;
				q._p._t2 = q._t1;
				if (q._n2 != null) {
					q._n2._p = q._p;
				}
			}
		}
		if (q._n1 != null) {
			stripUnaryNodes(q._n1);
		}
		if (q._n2 != null) {
			stripUnaryNodes(q._n2);
		}
	}

}

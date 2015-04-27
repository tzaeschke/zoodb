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
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.query.QueryParser.FNCT_OP;
import org.zoodb.internal.server.index.BitTools;
import org.zoodb.internal.util.DBLogger;

public class QueryOptimizer {
	
	private final ZooClassDef clsDef;
	
	/**
	 * A lookup map for all characters that indicate a (non-indexable) regex String. 
	 */
	private static final boolean[] REGEX_CHARS = new boolean[256];
	static {
		char[] regexChars = {'.', '\\', '+', '*', '[', '|', '$', '?'};
		for (char c: regexChars) {
			REGEX_CHARS[c] = true;
		}
	}
	
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
			if (!term.isRhsFixed() || term.isLhsFunction()) {
				//ignore terms with variable rhs and functios on the LHS
				//TODO we currently support only indexes on references, not on paths
				if (term.isLhsFunction()) {
					determineIndexToUseSubForQueryFunctions(minMap, maxMap, term.getLhsFunction());
				}
				continue;
			}
			ZooFieldDef f = term.getLhsFieldDef();
			if (f == null || !f.isIndexed()) {
				//ignore fields that are not index
				continue;
			}
			
			Long minVal = minMap.get(f);
			if (minVal == null) {
				//needs initialization
				//even if we don;t narrow the values, min/max allow ordered traversal
				minMap.put(f, f.getMinValue());
				maxMap.put(f, f.getMaxValue());
			}
			
			Object termVal = term.getValue(null);
			//TODO if(term.isRef())?!?!?!
			//TODO implement term.isIndexable() ?!?!?
			//TODO swap left/right side of query term such that indexed field is always on the left
			//     and the constant is on the right.
			Long value;
			
			switch (f.getJdoType()) {
			case PRIMITIVE:
				switch (f.getPrimitiveType()) {
				case BOOLEAN:   
					//pointless..., well pretty much, unless someone uses this to distinguish
					//very few 'true' from many 'false' or vice versa.
					continue;
				case DOUBLE: value = BitTools.toSortableLong(
						(termVal instanceof Double ? (double)termVal : (double)(float)termVal)); 
				break;
				case FLOAT: value = BitTools.toSortableLong(
						(termVal instanceof Float ? (float)termVal : (float)(double)termVal)); 
				break;
				case CHAR: value = (long)((Character)termVal).charValue();
				case BYTE:
				case INT:
				case LONG:
				case SHORT:	value = ((Number)termVal).longValue(); break;
				default: 				
					throw new IllegalArgumentException("Type: " + f.getPrimitiveType());
				}
				break;
			case STRING:
				value = BitTools.toSortableLong(
						termVal == QueryTerm.NULL ? null : (String)termVal); 
				break;
			case REFERENCE:
				value = (termVal == QueryTerm.NULL ? 
						BitTools.NULL : ((ZooPC)termVal).jdoZooGetOid());
				break;
			case DATE:
				value = (termVal == QueryTerm.NULL ? 0 : ((Date)termVal).getTime()); 
				break;
			default:
				throw new IllegalArgumentException("Type: " + f.getJdoType());
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
				setKeysForStringStartsWith((String) term.getValue(null), f, minMap, maxMap);
				break;
			default: 
				throw new IllegalArgumentException("Name: " + term.getOp());
			}
			
			//TODO take into account not-operators (x>1 && x<10) && !(x>5 && X <6) ??
			// -> Hopefully this optimization is marginal and negligible.
			//But it may break everything!
		}
		return createQueryAdvice(minMap, maxMap, queryTree);
	}
	
	private void determineIndexToUseSubForQueryFunctions( 
			IdentityHashMap<ZooFieldDef, Long> minMap, 
			IdentityHashMap<ZooFieldDef, Long> maxMap,
			QueryFunction fn) {
		
		//we can use indexes only for startsWith() and matches() 
		if (!FNCT_OP.STR_startsWith.equals(fn.op()) && !FNCT_OP.STR_matches.equals(fn.op())) {
			return;
		}
		
		//we can use index only when operatig on a local field 
		QueryFunction f0 = fn.getParams()[0];
		if (!FNCT_OP.FIELD.equals(f0.op())) {
			return;
		}
		
		if (f0.getParams()[0].op() != FNCT_OP.THIS) {
			//TODO we don't support path queries yet, i.e. the string field must belong to
			//the currently evaluated main-object, not to a referenced object.
			return;
		}
		
		ZooFieldDef f = f0.getFieldDef();
		if (f == null || !f.isIndexed()) {
			//ignore fields that are not index
			return;
		}
		
		QueryFunction f1 = fn.getParams()[1];
		if (!f1.isConstant()) {
			return;
		}
		Object param1 = f1.evaluate(null, null);   
		
		Long minVal = minMap.get(f);
		if (minVal == null) {
			//needs initialization
			//even if we don;t narrow the values, min/max allow ordered traversal
			minMap.put(f, f.getMinValue());
			maxMap.put(f, f.getMaxValue());
		}

		
		switch (fn.op()) {
		case STR_matches:
			String str = (String) param1;
			for (int i = 0; i < str.length(); i++) {
				char c = str.charAt(i);
				if (REGEX_CHARS[c]) {
					//if we have a regex that does not simply result in full match we
					//simply use the leading part for a startsWith() query.
					if (i == 0) {
						DBLogger.info("Ignoring index on String query because of regex characters.");
					}
					str = str.substring(0, i);
					setKeysForStringStartsWith(str, f, minMap, maxMap);
					return;
				}
			}
			long key = BitTools.toSortableLong(str);
			if (key > minMap.get(f)) {
				minMap.put(f, key);
			}
			if (key < maxMap.get(f)) {
				maxMap.put(f, key);
			}
			break;
		case STR_startsWith:
			setKeysForStringStartsWith((String) param1, f, minMap, maxMap);
			break;
		default: //nothing
		}
	}

	private void setKeysForStringStartsWith(String prefix, ZooFieldDef f,
			IdentityHashMap<ZooFieldDef, Long> minMap, 
			IdentityHashMap<ZooFieldDef, Long> maxMap) {
		long keyMin = BitTools.toSortableLongPrefixMinHash(prefix);
		long keyMax = BitTools.toSortableLongPrefixMaxHash(prefix);
		if (keyMin > minMap.get(f)) {
			minMap.put(f, keyMin);
		}
		if (keyMax < maxMap.get(f)) {
			maxMap.put(f, keyMax);
		}
	}
	
	private QueryAdvice createQueryAdvice(
			IdentityHashMap<ZooFieldDef, Long> minMap, 
			IdentityHashMap<ZooFieldDef, Long> maxMap, 
			QueryTreeNode queryTree) {
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
		while (q.isUnary() && q.n1 != null) {
			//this is a unary root node that shouldn't be one
			q.op = q.n1.op;
			q.n2 = q.n1.n2;
			q.t2 = q.n1.t2;
			q.t1 = q.n1.t1;
			q.n1 = q.n1.n1;
			q.relateToChildren();
		}
		//check unary nodes if they are not root / pull down leaf-unaries
		if (q.isUnary() && q.p != null) {
			if (q.p.n1 == q) {
				q.p.n1 = q.n1;
				q.p.t1 = q.t1;
				if (q.n1 != null) {
					q.n1.p = q.p;
				}
			} else {
				q.p.n2 = q.n1;
				q.p.t2 = q.t1;
				if (q.n2 != null) {
					q.n2.p = q.p;
				}
			}
		}
		if (q.n1 != null) {
			stripUnaryNodes(q.n1);
		}
		if (q.n2 != null) {
			stripUnaryNodes(q.n2);
		}
	}

}

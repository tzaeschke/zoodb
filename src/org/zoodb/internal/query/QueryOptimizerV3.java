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
import org.zoodb.internal.query.QueryOptimizerV4.IndexProposalSet;
import org.zoodb.internal.query.QueryOptimizerV4.MinMax;
import org.zoodb.internal.server.index.BitTools;

public class QueryOptimizerV3 {
	
	private final ZooClassDef clsDef;
	
	/**
	 * A lookup map for all characters that indicate a (non-indexable) regex String. 
	 */
	static final boolean[] REGEX_CHARS = new boolean[256];
	static {
		char[] regexChars = {'.', '\\', '+', '*', '[', '|', '$', '?'};
		for (char c: regexChars) {
			REGEX_CHARS[c] = true;
		}
	}
	
	public QueryOptimizerV3(ZooClassDef clsDef) {
		this.clsDef = clsDef;
	}
	
	/**
	 * Determine index to use.
	 * 
	 * Policy:
	 * 1) Check if index are available. If not, do not perform any further query analysis (for now)
	 *    Query rewriting may still be able to optimize really stupid queries.
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
	 * @param tree the root of the query tree
	 * @param params Query execution parameters
	 * @return Index to use.
	 */
	public List<QueryAdvice> determineIndexToUse(QueryTree tree, Object[] params) {
		QueryTreeNode queryTree = tree.getRootNode();
		if (queryTree == null) {
			return determineIndexToUseFN(tree, params);
		}
		List<QueryAdvice> advices = new LinkedList<>();
		List<ZooFieldDef> availableIndices = new LinkedList<>();
		for (ZooFieldDef f: clsDef.getAllFields()) {
			// We exclude dirty schemata here. They may be new or at least the index may be new, see issue #131.
			// TODO This is not ideal, we could allow using indexes if the relevant field+index hasn't changed.
			if (f.isIndexed() && !clsDef.jdoZooIsDirty()) {
				availableIndices.add(f);
			}
		}

		// step 1
		if (availableIndices.isEmpty()) {
			//no index usage
			advices.add( QueryAdvice.createEmpty(tree) );
			return advices;
		}
		
		//step 2 - sub-queries
		//We split the query tree at every OR into sub queries, such that every sub-query contains
		//the full query but only one side of every OR. All ORs are removed.
		//-> Optimization: We remove only (and split only at) ORs where at least on branch
		//   uses an index. TODO
		List<QueryTreeNode> subQueries = new LinkedList<>();
		subQueries.add(queryTree);
		queryTree.createSubs(subQueries);
		
//		System.out.println("Query2: " + queryTree.print());
		for (QueryTreeNode sq: subQueries) {
			optimize(sq);
//			System.out.println("Sub-query: " + sq.print());
		}
		//TODO filter out terms that cannot become true.
		//if none is left, return empty set.
		
		IdentityHashMap<ZooFieldDef, MinMax> minMaxMap = new IdentityHashMap<>();
		for (QueryTreeNode sq: subQueries) {
			advices.add(determineIndexToUseSub(QueryTree.create(sq), minMaxMap, params));
			minMaxMap.clear();
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
		IdentityHashMap<ZooFieldDef, TreeSet<QueryAdvice>> map = new IdentityHashMap<>();
		//sort QAs by index and by minValue
		for (QueryAdvice qa: advices) {
			TreeSet<QueryAdvice> subList = map.get(qa.getIndex());
			if (subList == null) {
				subList = new TreeSet<>(new AdviceComparator());
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
					prev.setMax(current.getMax(), current.isDependentOnParameter());
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
	 * @param minMaxMap min-max values
	 * @param params  parameters
	 * @return QueryAdvise
	 */
	private QueryAdvice determineIndexToUseSub(QueryTree queryTree, 
			IdentityHashMap<ZooFieldDef, MinMax> minMaxMap, Object[] params) {
		//TODO determine the Lists directly by assigning ZooFields to term during parsing?
		QueryTreeIterator iter = queryTree.termIterator();
		while (iter.hasNext()) {
			QueryTerm term = iter.next();
			if (!term.isRhsFixed() || term.isLhsFunction()) {
				//ignore terms with variable RHS and functions on the LHS
				//TODO we currently support only indexes on references, not on paths
				if (term.isLhsFunction()) {
					determineIndexToUseSubForQueryFunctions(minMaxMap, term.getLhsFunction(), params);
				}
				continue;
			}
			ZooFieldDef f = term.getLhsFieldDef();
			if (f == null || !f.isIndexed()) {
				//ignore fields that are not index
				continue;
			}
			
			MinMax minMaxVal = minMaxMap.get(f);
			if (minMaxVal == null) {
				//needs initialization
				//even if we don't narrow the values, min/max allow ordered traversal
				minMaxVal = new MinMax(f.getMinValue(), f.getMaxValue(), false, false);
				minMaxMap.put(f, minMaxVal);
			}
			
			Object termVal = term.getValue(null, params);
			boolean isDependentOnParam = term.isDependentOnParameter();
			//TODO if(term.isRef())?!?!?!
			//TODO implement term.isIndexable() ?!?!?
			//TODO swap left/right side of query term such that indexed field is always on the left
			//     and the constant is on the right.
			long value;
			
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
				case CHAR: value = (long)((Character)termVal).charValue(); break;
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
				minMaxVal.adjustMin(value, isDependentOnParam);
				minMaxVal.adjustMax(value, isDependentOnParam);
				break;
			}
			case L:
				minMaxVal.adjustMax(value - 1, isDependentOnParam);
				break;
			case LE: 				
				minMaxVal.adjustMax(value, isDependentOnParam);
				break;
			case A: 
				minMaxVal.adjustMin(value + 1, isDependentOnParam);
				break;
			case AE:
				minMaxVal.adjustMin(value, isDependentOnParam);
				break;
			case NE:
			case STR_matches:
			case STR_contains_NON_JDO:
			case STR_endsWith:
				//ignore
				break;
			case STR_startsWith:
				setKeysForStringStartsWith((String) term.getValue(null, params), f, minMaxMap,
					isDependentOnParam);
				break;
			default: 
				throw new IllegalArgumentException("Name: " + term.getOp());
			}
			
			//TODO take into account not-operators (x>1 && x<10) && !(x>5 && X <6) ??
			// -> Hopefully this optimization is marginal and negligible.
			//But it may break everything!
		}
		return createQueryAdvice(minMaxMap, queryTree);
	}
	
	public List<QueryAdvice> determineIndexToUseFN(QueryTree tree, Object[] params) {
		QueryFunction queryTree = tree.getRootFn();
		List<QueryAdvice> advices = new LinkedList<>();
		List<ZooFieldDef> availableIndices = new LinkedList<>();
		for (ZooFieldDef f: clsDef.getAllFields()) {
			if (f.isIndexed()) {
				availableIndices.add(f);
			}
		}

		// step 1
		if (availableIndices.isEmpty()) {
			//no index usage
			advices.add( QueryAdvice.createEmpty(tree) );
			return advices;
		}
		
		//step 2 - sub-queries
		//We split the query tree at every OR into sub queries, such that every sub-query contains
		//the full query but only one side of every OR. All ORs are removed.
		//-> Optimization: We remove only (and split only at) ORs where at least on branch
		//   uses an index. TODO
		List<QueryFunction> subQueries = new LinkedList<>();
		subQueries.add(queryTree);

		//TODO filter out terms that cannot become true.
		//if none is left, return empty set.
		
		IdentityHashMap<ZooFieldDef, MinMax> minMaxMap = new IdentityHashMap<>();
		for (QueryFunction sq: subQueries) {
			determineIndexToUseSubForQueryFunctions(minMaxMap, sq, params);
			QueryAdvice qa = createQueryAdvice(minMaxMap, QueryTree.create(queryTree));
			advices.add(qa);
			minMaxMap.clear();
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
	
	private void determineIndexToUseSubForQueryFunctions( 
			IdentityHashMap<ZooFieldDef, MinMax> minMaxMap,
			QueryFunction fn, Object[] params) {
		
		IndexProposalSet[] vars = new IndexProposalSet[1];
		vars[0] = new IndexProposalSet();
		vars[0].minMaxMap = minMaxMap;
		fn.determineIndexToUseSubForQueryFunctions(vars, params);
	}
	
	private void setKeysForStringStartsWith(String prefix, ZooFieldDef f,
			IdentityHashMap<ZooFieldDef, MinMax> minMaxMap, boolean isParam) {
		long keyMin = BitTools.toSortableLongPrefixMinHash(prefix);
		long keyMax = BitTools.toSortableLongPrefixMaxHash(prefix);
		MinMax minMax = minMaxMap.get(f);
		minMax.adjustMin(keyMin, isParam);
		minMax.adjustMax(keyMax, isParam);
	}
	
	private QueryAdvice createQueryAdvice(
			IdentityHashMap<ZooFieldDef, MinMax> minMaxMap, 
			QueryTree queryTree) {
		if (minMaxMap.isEmpty()) {
			//return default query
			return QueryAdvice.createEmpty(queryTree);
		}
		
		//the advised index to use...
		// start with first
		ZooFieldDef def = minMaxMap.keySet().iterator().next();
		QueryAdvice qa = QueryAdvice.createForIndex(queryTree, def, minMaxMap.get(def) );
		
		//only one index left? -> Easy!!!
		//TODO well, better not use it if it covers the whole range? Maybe for sorting?
		if (minMaxMap.size() == 1) {
			qa.setIndex( minMaxMap.keySet().iterator().next() );
			return qa;
		}
		
		for (Map.Entry<ZooFieldDef, MinMax> me2: minMaxMap.entrySet()) {
			long min2 = me2.getValue().min;
			long max2 = me2.getValue().max;
			//TODO fix for very large values
			if ((max2-min2) < (qa.getMax() - qa.getMin())) {
				qa.setIndex( me2.getKey() );
				qa.setMin( min2, me2.getValue().isMinDependentOnParameter );
				qa.setMax( max2, me2.getValue().isMaxDependentOnParameter );
			}
		}
		
		if (qa.getIndex().isString()) {
			//For String we have to extend the range because of the trailing hashcode
			qa.setMin(BitTools.getMinPosInPage(qa.getMin()), qa.isDependentOnParameter());
			qa.setMax(BitTools.getMaxPosInPage(qa.getMax()), qa.isDependentOnParameter());
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

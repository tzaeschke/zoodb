package org.zoodb.jdo.internal.query;

import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.ZooFieldDef;
import org.zoodb.jdo.internal.server.index.BitTools;
import org.zoodb.jdo.stuff.DatabaseLogger;

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
		List<QueryTreeNode> subQueriesCandidates = new LinkedList<QueryTreeNode>();
		subQueriesCandidates.add(queryTree);
//		System.out.println("Query1: " + queryTree.print());
		while (!subQueriesCandidates.isEmpty()) {
			subQueries.add(subQueriesCandidates.remove(0).createSubs(subQueriesCandidates));
		}
		
//		System.out.println("Query2: " + queryTree.print());
		for (QueryTreeNode sq: subQueries) {
			optimize(sq);
//			System.out.println("Sub-query: " + sq.print());
		}
		//TODO filter out terms that can not become true.
		//if none is left, return empty set.
		
		for (QueryTreeNode sq: subQueries) {
			advices.add(determineIndexToUseSub(sq));
		}
		
		//TODO merge queries
		//E.g.:
		// - if none uses an index (or at least one doesn't), return only the full query
		// - if ranges overlap, try to merge?
		
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
		long globalMin = Long.MAX_VALUE;
		long globalMax = Long.MIN_VALUE;
		//if they overlap, we should merge them to void duplicate loading effort and results.
		//if they don't overlap, we don't have to care about either.
		//-> assuming they all use the same index... TODO?
		
		return advices;
	}
	
		
	/**
	 * 
	 * @param queryTree This is a sub-query that does not contain OR operands.
	 * @return QueryAdvise
	 */
	private QueryAdvice determineIndexToUseSub(QueryTreeNode queryTree) {
		//TODO determine this List directly by assigning ZooFields to term during parsing?
		Map<ZooFieldDef, Long> minMap = new IdentityHashMap<ZooFieldDef, Long>();
		Map<ZooFieldDef, Long> maxMap = new IdentityHashMap<ZooFieldDef, Long>();
		QueryTreeIterator iter = queryTree.termIterator();
		while (iter.hasNext()) {
			QueryTerm term = iter.next();
			ZooFieldDef f = term.getFieldDef();
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
			if (term.getValue() instanceof Number) {
				value = ((Number)term.getValue()).longValue();
			} else if (term.getValue() instanceof String) {
				value = BitTools.toSortableLong((String) term.getValue());
			} else if (term.getValue() instanceof Boolean) {
				//pointless..., well pretty much, unless someone uses this to distinguish
				//very few 'true' from many 'false'or vice versa.
				continue;
			} else {
				throw new IllegalArgumentException("Type: " + term.getValue().getClass());
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
				//ignore
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
		
		//the adviced index to use...
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
		
		for (ZooFieldDef d2: minMap.keySet()) {
			long min2 = minMap.get(d2);
			long max2 = maxMap.get(d2);
			//TODO fix for very large values
			if ((max2-min2) < (qa.getMax() - qa.getMin())) {
				qa.setIndex( d2 );
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

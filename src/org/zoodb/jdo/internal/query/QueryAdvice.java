package org.zoodb.jdo.internal.query;

import org.zoodb.jdo.internal.ZooFieldDef;

/**
 * This class holds results from the query analyzer for the query executor.
 * - the query
 * - Index to use (if != null)
 * - min/max values of that index
 * - ascending/descending? 
 * 
 * @author Tilmann Zaeschke
 */
public final class QueryAdvice {
	private final QueryTreeNode query;
	private ZooFieldDef index;
	private long min;
	private long max;
	private boolean ascending;
	
	public QueryAdvice(QueryTreeNode queryTree) {
		this.query = queryTree;
	}

	public ZooFieldDef getIndex() {
		return index;
	}

	void setIndex(ZooFieldDef index) {
		this.index = index;
	}

	public long getMin() {
		return min;
	}

	void setMin(long min) {
		this.min = min;
	}

	public long getMax() {
		return max;
	}

	void setMax(long max) {
		this.max = max;
	}

	public boolean isAscending() {
		return ascending;
	}

	void setAscending(boolean ascending) {
		this.ascending = ascending;
	}

	public QueryTreeNode getQuery() {
		return query;
	}
	
	
}
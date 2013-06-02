/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
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
/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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

public abstract class QueryOptimizer {
	
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
	 * @return Index to use.
	 */
	public abstract List<QueryAdvice> determineIndexToUse(QueryTree tree);

}

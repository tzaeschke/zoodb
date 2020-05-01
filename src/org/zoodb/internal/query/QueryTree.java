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

import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.query.QueryExecutor.VariableInstance;

public class QueryTree {

	public static final long NO_RANGE = -1;
	
	private final QueryFunction rootFn;
	private final QueryTreeNode rootNode;

	private long rangeMin;
	private long rangeMax;
	private ParameterDeclaration rangeMinParameter = null;
	private ParameterDeclaration rangeMaxParameter = null;
	private boolean requiresReoptimizationWhenParamsChange = false;
	
	QueryTree(QueryFunction root, long rangeMin, long rangeMax, 
			ParameterDeclaration rangeMinParameter, ParameterDeclaration rangeMaxParameter) {
		this(root, null, rangeMin, rangeMax, rangeMinParameter, rangeMaxParameter);
	}
	
	QueryTree(QueryTreeNode root, long rangeMin, long rangeMax, 
			ParameterDeclaration rangeMinParameter, ParameterDeclaration rangeMaxParameter) {
		this(null, root, rangeMin, rangeMax, rangeMinParameter, rangeMaxParameter);
	}
	
	private QueryTree(QueryFunction root, QueryTreeNode rootNode, 
			long rangeMin, long rangeMax, 
			ParameterDeclaration rangeMinParameter, ParameterDeclaration rangeMaxParameter) {
		this.rootFn = root;
		this.rootNode = rootNode;
		this.rangeMin = rangeMin;
		this.rangeMax = rangeMax;
		this.rangeMinParameter = rangeMinParameter;
		this.rangeMaxParameter = rangeMaxParameter;
	}
	
	long getRangeMin(long rangeMin, Object[] params) {
		//If we have a parameter, use the parameter
		if (rangeMinParameter != null) {
			rangeMin = TypeConverterTools.toLong(rangeMinParameter.getValue(params));
		}
		if (rangeMin != NO_RANGE) {
			return rangeMin;
		}
		return this.rangeMin != NO_RANGE ? this.rangeMin : 0;
	}
	
	long getRangeMax(long rangeMax, Object[] params) {
		if (rangeMaxParameter != null) {
			rangeMax = TypeConverterTools.toLong(rangeMaxParameter.getValue(params));
		}
		if (rangeMax != NO_RANGE) {
			return rangeMax;
		}
		return this.rangeMax != NO_RANGE ? this.rangeMax : 0;
	}

	public String print() {
		return rootFn != null ? rootFn.toString() : rootNode.print();
	}

	QueryFunction getRootFn() {
		return rootFn;
	}

	QueryTreeNode getRootNode() {
		return rootNode;
	}

	public QueryTreeIterator termIterator() {
		return rootNode.termIterator();
	}

	public boolean evaluate(Object o, Object[] params) {
		return rootFn != null 
				? rootFn.evaluateBool(o, o, null, params) 
				: rootNode.evaluate(o, params);
	}

	public static QueryTree create(QueryFunction queryFn) {
		return new QueryTree(queryFn, NO_RANGE, NO_RANGE, null, null);
	}

	public static QueryTree create(QueryTreeNode queryNode) {
		return new QueryTree(queryNode, NO_RANGE, NO_RANGE, null, null);
	}
	
	public List<QueryAdvice> executeOptimizer(ZooClassDef candClsDef, Object[] paramValues) {
		return new QueryOptimizerV3(candClsDef).determineIndexToUse(this, paramValues); 
	}
	
	public VariableInstance[] executeOptimizerV4(ZooClassDef candClsDef, 
			List<QueryVariable> variables, Object[] executionParams) {
		return QueryOptimizerV4.optimizeQuery(this, candClsDef, variables, executionParams); 
	}

	public boolean requiresRerunForChangedParams() {
		if (rootNode != null) {
			throw new UnsupportedOperationException();
		}
		return requiresReoptimizationWhenParamsChange;
	}

	public void setRequiresReoptimizationWhenParamsChange(boolean requiresRerun) {
		this.requiresReoptimizationWhenParamsChange = requiresRerun;
	}
}

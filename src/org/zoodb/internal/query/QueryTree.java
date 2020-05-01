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

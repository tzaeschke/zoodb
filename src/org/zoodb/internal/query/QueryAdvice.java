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

import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.query.QueryOptimizerV4.MinMax;

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
	
	static final double COST_EXTENT = 100000;
	static final double COST_CONTAINS = 10;
	static final double COST_INDEX_RANGE = 100;
	static final double COST_INDEX_EQUALS = 10;
	static final double COST_EQUALS = 1;

	public enum Type {
		EXTENT(COST_EXTENT),
		INDEX_RANGE(COST_INDEX_RANGE),
		INDEX_EQUALS(COST_INDEX_EQUALS),
		COLLECTION(COST_CONTAINS),
		IDENTITY(COST_EQUALS);
		
		private double cost;
		Type(double cost) {
			this.cost = cost;
		}
	}
	
	private final QueryTree query;
	private ZooFieldDef index;
	private long min;
	private long max;
	private boolean ascending;
	//Indicates that we can use a collection to constrain the query candidates
	private QueryFunction collectionConstraint;
	//Indicates that we use '==' to set the variable value
	private QueryFunction identityConstraint;
	private final Type type;
	//Dow this depend on a query execution parameter?
	private boolean isDependentOnParameter = false;
	
	private QueryAdvice(QueryTree queryTree, Type type) {
		this.query = queryTree;
		this.type = type;
	}

	public static QueryAdvice createForIndex(QueryTree queryTree, ZooFieldDef index, MinMax minMax) {
		QueryAdvice a = new QueryAdvice(queryTree, minMax.min == minMax.max ? Type.INDEX_EQUALS : Type.INDEX_RANGE);
		a.setIndex(index);
		a.setMin(minMax.min, minMax.isDependentOnParameter());
		a.setMax(minMax.max, minMax.isDependentOnParameter());
		a.isDependentOnParameter |= minMax.isDependentOnParameter();
		return a;
	}
	
	public static QueryAdvice createForCollection(QueryTree queryTree, QueryFunction collection) {
		QueryAdvice a = new QueryAdvice(queryTree, Type.COLLECTION);
		a.setCollectionConstraint(collection);
		return a;
	}
	
	public static QueryAdvice createForIdentity(QueryTree queryTree, QueryFunction identity,
			boolean isDependentOnParameter) {
		QueryAdvice a = new QueryAdvice(queryTree, Type.IDENTITY);
		a.setIdentityConstraint(identity, isDependentOnParameter);
		return a;
	}
	
	@Deprecated
	public static QueryAdvice createEmpty(QueryTree queryTree) {
		return new QueryAdvice(queryTree, Type.EXTENT);
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

	void setMin(long min, boolean isDependentOnParameter) {
		this.min = min;
		this.isDependentOnParameter |= isDependentOnParameter;
	}

	public long getMax() {
		return max;
	}

	void setMax(long max, boolean isDependentOnParameter) {
		this.max = max;
		this.isDependentOnParameter |= isDependentOnParameter;
	}

	public boolean isAscending() {
		return ascending;
	}

	void setAscending(boolean ascending) {
		this.ascending = ascending;
	}

	public QueryTree getQuery() {
		return query;
	}

	public void setCollectionConstraint(QueryFunction queryFunction) {
		this.collectionConstraint = queryFunction;
	}

	public QueryFunction getCollectionConstraint() {
		return this.collectionConstraint;
	}

	public boolean hasCollectionConstraint() {
		return collectionConstraint != null;
	}

	public void setIdentityConstraint(QueryFunction identityConstraint,
			boolean isDependentOnParameter) {
		this.identityConstraint = identityConstraint;
		this.isDependentOnParameter |= isDependentOnParameter;
	}
	
	public QueryFunction getIdentityConstraint() {
		return this.identityConstraint;
	}

	public boolean hasIdentityConstraint() {
		return identityConstraint != null;
	}
	
	public boolean isDependentOnParameter() {
		return isDependentOnParameter;
	}
	
	double getCost() {
		return type.cost;
	}
	
	Type getType() {
		return type;
	}
}

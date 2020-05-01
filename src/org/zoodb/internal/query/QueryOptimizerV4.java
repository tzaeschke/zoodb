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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.query.QueryExecutor.VariableInstance;
import org.zoodb.internal.query.QueryFunction.Constraint;
import org.zoodb.internal.query.QueryVariable.VarDeclaration;
import org.zoodb.internal.server.index.BitTools;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.jdo.impl.QueryImpl;

public class QueryOptimizerV4 {
	
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
	
	private QueryOptimizerV4() {
		//not used
	}
	
	static class MinMax {
		long min = Long.MIN_VALUE;
		long max = Long.MAX_VALUE;
		boolean isMinDependentOnParameter = false;
		boolean isMaxDependentOnParameter = false;
		public MinMax(long minValue, long maxValue,
				boolean isMinDependentOnParameter, boolean isMaxDependentOnParameter) {
			this.min = minValue;
			this.max = maxValue;
			this.isMinDependentOnParameter = isMinDependentOnParameter;
			this.isMinDependentOnParameter = isMaxDependentOnParameter;
		}
		public void adjustMin(long min, boolean isDependentOnParameter) {
			if (min > this.min) {
				this.min = min;
				this.isMinDependentOnParameter = isDependentOnParameter;
			}
		}
		public void adjustMax(long max, boolean isDependentOnParameter) {
			if (max < this.max) {
				this.max = max;
				this.isMaxDependentOnParameter = isDependentOnParameter;
			}
		}
		public boolean isDependentOnParameter() {
			return isMinDependentOnParameter 
					|| isMaxDependentOnParameter;
		}
	}
	
	/**
	 * Index proposal.
	 * Cumulative indexes occur when the query contains OR terms. If both sides of
	 * an OR term contain indexable fields then the query can use a cumulative index
	 * consisting of the two partial indexes for each side of the OR term.
	 * Example: x==5 || y==3.
	 * (Cumulative) indexes can be nested.  
	 */
	static class IndexProposalSet {
		//proposals for concrete indexes (min/max for specific fields)
		IdentityHashMap<ZooFieldDef, MinMax> minMaxMap = new IdentityHashMap<>();
		
		//List of 'collectionConstraints' through Map.contains()/Collections.contains().
		final List<QueryFunction> collectionConstraints = new ArrayList<>();

		//List of 'identityConstraints' through '=='.
		final List<QueryFunction> identityConstraints = new ArrayList<>();

		//List of proposals for cumulative indexes.
		//Each 'inner' list represents a cumulative index
		final List<List<IndexProposalSet>> indexesChoicesToMerge = new ArrayList<>();
		//Track whether the values are dependent on query parameters,
		//because we may have to reoptimize the query if the parameters change.
		//YES! WE KNOW THIS IS NOT QUITE CORERECT! In theory, the parameterized 
		//value could be inbetween a constant min/max value and we would still flag 
		//it as 'dependent on parameter'. However this case should be quite rare...
		private boolean isDependentOnParameter = false;
		
		public void addCumulative(List<IndexProposalSet> proposal) {
			//This is not quite correct, see above
			for (int i = 0; i < proposal.size(); i++) {
				this.isDependentOnParameter |= proposal.get(i).isDependentOnParameter;
			}
			indexesChoicesToMerge.add(proposal);
		}
		
		public void addIdentityConstraint(QueryFunction fn, boolean isParameter) {
			//Hmm, identity constraint dependent on parameter? Do we need this?
			this.isDependentOnParameter |= isParameter;
			identityConstraints.add(fn);
		}
		
		public void addCollectionConstraint(QueryFunction fn) {
			collectionConstraints.add(fn);
		}

		public void initMinMax(ZooFieldDef zField) {
			minMaxMap.computeIfAbsent(zField, 
					f -> new MinMax(f.getMinValue(), f.getMaxValue(), false, false));
		}
		
		public void addMin(ZooFieldDef zField, long newMin, boolean isParameter) {
			//This is not quite correct, see above
			this.isDependentOnParameter |= isParameter;
			minMaxMap.get(zField).adjustMin(newMin, isParameter);
		}
		
		public void addMax(ZooFieldDef zField, long newMax, boolean isParameter) {
			//This is not quite correct, see above
			this.isDependentOnParameter |= isParameter;
			minMaxMap.get(zField).adjustMax(newMax, isParameter);
		}
	}
	
	
	public static VariableInstance[] optimizeQuery(QueryTree tree, ZooClassDef clsDef,
			List<QueryVariable> variableDecls, Object[] params) {
		
		QueryVariable mainVarDecl = new QueryVariable(clsDef.getJavaClass(), "$_mainVar_$", VarDeclaration.ROOT, 0);
		mainVarDecl.setTypeDef(clsDef);
		VariableInstance mainVar = new VariableInstance(mainVarDecl);

//		QueryVariable[] varsDecl = variableDecls.toArray(new QueryVariable[variableDecls.size() + 1]);
//		System.arraycopy(varsDecl, 0, varsDecl, 1, varsDecl.length - 1);
//		varsDecl[0] = mainVarDecl;

		//analyze query tree
		VariableInstance[] variableIns = new VariableInstance[variableDecls.size() + 1];
		//root var
		variableIns[0] = mainVar;
		for (int v = 1; v < variableIns.length; v++) {
			variableIns[v] = new VariableInstance(variableDecls.get(v - 1));
		}


		//Detect index opportunities
		QueryFunction root = tree.getRootFn();
		IndexProposalSet[] proposalsForEachVar = new IndexProposalSet[variableIns.length];
		root.determineIndexToUseSubForQueryFunctions(proposalsForEachVar, params);
		for (int i = 0; i < proposalsForEachVar.length; i++) {
			VariableInstance var = variableIns[i];
			List<QueryAdvice> result = var.advices;
			createQueryAdvice(proposalsForEachVar[i], var, tree, result);
			var.prepareForUsage();
			if (var.var.getTypeDef() == null && var.getAdvices().isEmpty()) {
				throw DBLogger.newUser("Queries need identity constraints, such as "
						+ "'==' or '.contains()', for variables of type '" 
						+ var.var.getType().getName() + "'");
			}
		}

		//Reorder the tree to ensure that Variables are assigned before they are used.
		//Also: optimize for best index usage.
		//TODO remove
//		boolean[] constrainedVars = new boolean[variableIns.length];
//		if (!root.resolveMissingConstraintsByReordering(constrainedVars)) {
//			for (int i = 0; i < constrainedVars.length; i++) {
//				if (!constrainedVars[i]) {
//					QueryVariable qv = variableIns[i].var;
//					if (qv.getTypeDef() == null) {
//						throw DBLogger.newUser("Query has unconstrained query variable '" + 
//								variableIns[i].var.getName() + "':" + root.toString());
//					}
//					if (!variableIns[i].hasAdvices()) {
//						if (i > 0) {
//							QueryImpl.LOGGER.warn("Query has unindexed variables: '" + 
//									qv.getName() + "':" + root.toString());
//						} else {
//							QueryImpl.LOGGER.warn("Query 'from' class could not find any index: '" + 
//									qv.getTypeDef().getClassName() + "':" + root.toString());
//						}
//					}
//				}
//			}
//			throw new IllegalStateException();
//		}
		
		double[] constrainedVars = new double[variableIns.length];
		Arrays.fill(constrainedVars, Constraint.UNUSED.cost);
		double costEstimate = root.resolveMissingConstraintsByReordering(constrainedVars, variableIns, false);
		if (costEstimate >= Constraint.OBJ_UNCONSTRAINED.cost) {
			//TODO throw error only on costEstimate too big.. -> why is it 1?
			for (int i = 0; i < constrainedVars.length; i++) {
				if (constrainedVars[i] >= Constraint.OBJ_UNCONSTRAINED.cost) {
					QueryVariable qv = variableIns[i].var;
					if (qv.getTypeDef() == null) {
						throw DBLogger.newUser("Query has unconstrained query variable '" + 
								variableIns[i].var.getName() + "':" + root.toString());
					}
					if (!variableIns[i].hasAdvices()) {
						if (i > 0) {
							QueryImpl.LOGGER.warn("Query has unindexed variables: '" + 
									qv.getName() + "':" + root.toString());
						} else {
							QueryImpl.LOGGER.warn("Query 'from' class could not find any index: '" + 
									qv.getTypeDef().getClassName() + "':" + root.toString());
						}
					}
				}
			}
		}
		if (DBLogger.LOGGER.isDebugEnabled()) {
			DBLogger.LOGGER.debug("REORDERED (est. cost=" + costEstimate + "): " + root.toString());
		}
		//TODO
		//System.out.println("REORDERED (est. cost=" + costEstimate + "): " + root.toString());

		
		//Merging INDEX RANGES in queries:
		//We should only merge if ranges overlap. Otherwise we should _not_ merge 
		//for example  x==3 || x=300000 should not be merged because it gets expensive.
		
		//Merging RESULTS in queries:
		//This cannot be avoided if:
		//- indexes are on different fields
		//- ranges are defined via parameters
		
		//Simplified:
		//For now we always merge results if we have more than one QueryAdvice.
		
		//This could be slightly optimized by trying to merge overlaps.
		//This would only help if the query has no overlaps and no parameters are used.
		
		//Determine whether we need to optimize every time the parameters change.
		//We always need to re-optimize because the min/max value in the QueryAdvice
		//depend on the parameters.
		if (params.length > 0) {
			for (int iVI = 0; iVI < variableIns.length; iVI++) {
				List<QueryAdvice> qas = variableIns[iVI].advices;
				for (int iQA = 0; iQA < qas.size(); iQA++) {
					QueryAdvice qa = qas.get(iQA);
					if (qa.getIndex() != null && qa.isDependentOnParameter()) {
						tree.setRequiresReoptimizationWhenParamsChange(true);
						iVI = variableIns.length;
						break;
					}
				}
			}
		}
		
		return variableIns;
	}
	
	private static void createQueryAdvice(IndexProposalSet proposalsForVar, 
			VariableInstance var, QueryTree queryTree, List<QueryAdvice> result) {
		if (proposalsForVar == null) {
			//No index proposals found
			return;
		}
		
		QueryAdvice qa = createAdviceFromMap(queryTree, proposalsForVar, var);
		if (qa != null) {
			result.add(qa);
			return;
		}
		//TODO we should always do that and compare them to the min/max indexes!
		//analyze cumulative indexes
		createAdviceFromCumulative(queryTree, proposalsForVar, var, result);
	}
	
	private static QueryAdvice createAdviceFromMap(QueryTree queryTree,
			IndexProposalSet proposalsForVar, VariableInstance var) {
		//TODO
		//TODO
		//TODO
		//TODO
		//TODO Improve proposal cost calculation
		
		if (!proposalsForVar.collectionConstraints.isEmpty()) {
			//TODO get all constrains, and compare them (how?)
			return QueryAdvice.createForCollection(queryTree, 
					proposalsForVar.collectionConstraints.get(0));
		}

		//Use QA to enforce assignment or throw error if no '=='was used.
		if (!(proposalsForVar.identityConstraints.isEmpty())) {
			//TODO get all constrains, and compare them (how?)
			return QueryAdvice.createForIdentity(queryTree, 
					proposalsForVar.identityConstraints.get(0),
					proposalsForVar.isDependentOnParameter);
		}
		if (var.var.getTypeDef() == null) {
			throw DBLogger.newUser("Queries need identity constraints, such as "
					+ "'==' or '.contains()', for variables of type: " 
					+ var.var.getType().getName() + "'");
		}
		
		//TODO use [] with field IDs instead of maps
		IdentityHashMap<ZooFieldDef, MinMax> minMaxMap = proposalsForVar.minMaxMap;
		if (minMaxMap.isEmpty()) {
			//return default query
			return null;
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
			//TODO fix for very large values, for example: cast to 'double' and recheck
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

		return qa;
	}

	private static void createAdviceFromCumulative(QueryTree queryTree,
			IndexProposalSet proposalsForVar, VariableInstance var,
			List<QueryAdvice> result) {
		if (proposalsForVar.indexesChoicesToMerge == null 
				|| proposalsForVar.indexesChoicesToMerge.isEmpty()) {
			return;
		}
		
		for (int c = 0; c < proposalsForVar.indexesChoicesToMerge.size(); c++) {
			List<IndexProposalSet> choice = proposalsForVar.indexesChoicesToMerge.get(c);
			if (!choice.isEmpty()) { 
				for (int i = 0; i < choice.size(); i++) {
					IndexProposalSet prop = choice.get(i); 
					createQueryAdvice(prop, var, queryTree, result);
				}
				//TODO: compare with other proposals before returning!
				return;
			}
		}
	}
}

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.jdo.ObjectState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.Session;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.client.session.ClientSessionCache;
import org.zoodb.internal.query.QueryAdvice.Type;
import org.zoodb.internal.util.CloseableIterator;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.ObjectIdentitySet;
import org.zoodb.internal.util.Pair;
import org.zoodb.internal.util.SynchronizedROCollection;
import org.zoodb.tools.DBStatistics;
import org.zoodb.tools.DBStatistics.STATS;

public class QueryExecutor {


	public static final Logger LOGGER = LoggerFactory.getLogger(QueryExecutor.class);

	private final transient Session pm;
	private final String filter;
	private Class<?> candCls = ZooPC.class; //TODO good default?
	private transient ZooClassDef candClsDef = null;
	private boolean unique = false;
	private boolean subClasses = true;
	private boolean isDummyQuery = false;
	
	private List<Pair<ZooFieldDef, Boolean>> ordering = new ArrayList<>();

	private QueryTree queryTree;
	private ArrayList<ParameterDeclaration> parameters;
	private ArrayList<QueryVariable> variableDeclarations;
	private VariableInstance[] optimizerResult;
	
	public QueryExecutor(Session pm, 
			String filter,
			Class<?> candCls,
			ZooClassDef candClsDef,
			boolean unique,
			boolean subClasses,
			boolean isDummyQuery,
			List<Pair<ZooFieldDef, Boolean>> ordering,
			QueryTree queryTree,
			ArrayList<ParameterDeclaration> parameters,
			ArrayList<QueryVariable> variableDeclarations) {
		this.pm = pm;
		this.filter = filter;
		this.candCls = candCls == null ? ZooPC.class : candCls; //TODO good default?
		this.candClsDef = candClsDef;
		this.unique = unique;
		this.subClasses = subClasses;
		this.isDummyQuery = isDummyQuery;
		this.ordering = ordering;
		this.queryTree = queryTree;
		this.parameters = parameters;	
		this.variableDeclarations = variableDeclarations;	
	}
	
	public Object runWithExtent(Collection<Object> ext, long rangeMin, long rangeMax, 
			String resultSettings, Class<?> resultClass) {
		return postProcessV4(ext.iterator(), rangeMin, rangeMax, resultSettings, resultClass);
	}
	
	/**
	 * Parameters are the always modifiable parameters: range, resultClass and ignoreCache
	 *  (see JDO spec 14.6, explanation of set(Un)Modifiable, p173 in 3.1).
	 * @param ext Extent to run the query on. May be null.
	 * @param rangeMin Range minimum
	 * @param rangeMax Range maximum
	 * @param resultSettings Result settings string
	 * @param resultClass Result class
	 * @param ignoreCache Whether to ignore cached objects
	 * @param params Query execution parameters
	 * @return result
	 */
	public Object runQuery(Iterable<?> ext, long rangeMin, long rangeMax, String resultSettings, 
			Class<?> resultClass, boolean ignoreCache, Object[] params) {
		if (DBStatistics.isEnabled()) {
			pm.statsInc(STATS.QU_EXECUTED_TOTAL);
		}
		long t1 = System.nanoTime();
		try {
			pm.lock();
			pm.checkActiveRead();
			if (isDummyQuery) {
				//empty result if no schema is defined (auto-create schema)
				//TODO check cached objects
				return new LinkedList<Object>();
			}
			if (queryTree.getRootFn() != null) {
				return runQueryV4(ext, rangeMin, rangeMax, resultSettings, resultClass, ignoreCache,
						params);
			}
			return runQueryV3(ext, rangeMin, rangeMax, resultSettings, resultClass, ignoreCache,
					params);

		} finally {
			pm.unlock();
			if (LOGGER.isInfoEnabled()) {
				long t2 = System.nanoTime();
				LOGGER.info("query.execute(): Time={}ns; Class={}; filter={}", (t2-t1), candCls, filter);
			}
		}
	}
	
	private Object runQueryV3(Iterable<?> ext, long rangeMin, long rangeMax, String resultSettings, 
			Class<?> resultClass, boolean ignoreCache, Object[] params) {
		rangeMin = queryTree.getRangeMin(rangeMin, params);
		rangeMax = queryTree.getRangeMax(rangeMax, params);
		
		//assign parameters
		assignParametersToQueryTree();
		//This is only for indices, not for given extents
		List<QueryAdvice> indexToUse = queryTree.executeOptimizer(candClsDef, params);

		//TODO can also return a list with (yet) unknown size. In that case size() should return
		//Integer.MAX_VALUE (JDO 2.2 14.6.1)
		ArrayList<Object> ret = new ArrayList<Object>();
		for (QueryAdvice qa: indexToUse) {
			applyQueryOnExtentV3(ext, ret, qa, ignoreCache, params);
		}

		//Now check if we need to check for duplicates, i.e. if multiple indices were used.
		for (QueryAdvice qa: indexToUse) {
			if (qa.getIndex() != indexToUse.get(0).getIndex()) {
				LOGGER.warn("Merging query results(A)!");
				ObjectIdentitySet<Object> ret2 = new ObjectIdentitySet<Object>();
				ret2.addAll(ret);
				return postProcessV3(ret2, rangeMin, rangeMax, resultSettings, resultClass);
			}
		}

		//If we have more than one sub-query, we need to merge anyway, because the result sets may
		//overlap. 
		//TODO implement merging of sub-queries!!!
		if (indexToUse.size() > 1) {
			LOGGER.warn( "Merging query results(B)!");
			ObjectIdentitySet<Object> ret2 = new ObjectIdentitySet<Object>();
			ret2.addAll(ret);
			return postProcessV3(ret2, rangeMin, rangeMax, resultSettings, resultClass);
		}

		return postProcessV3(ret, rangeMin, rangeMax, resultSettings, resultClass);
	}

	public static class VariableInstance {
		QueryVariable var;
		final List<QueryAdvice> advices;
		Object value = null;
		private Iterator<?> iterator; 
		//Shortcut that indicates that the QueryAdvices recommend
		//using a collection constraint (iterator of collection of candidate 
		//field with 'contains()')
		boolean hasCollectionConstraint;
		QueryAdvice.Type type;
		VariableInstance(QueryVariable var) {
			this(var, new ArrayList<>());
		}
		private VariableInstance(QueryVariable var, List<QueryAdvice> advices) {
			this.var = var;
			this.advices = advices;
		}
		public void setIterator(Iterator<?> iter) {
			this.iterator = iter;
		}
		public Iterator<?> iter() {
			return this.iterator;
		}
		public List<QueryAdvice> getAdvices() {
			return advices;
		}
		public boolean hasAdvices() {
			return !advices.isEmpty();
		}
		void prepareForUsage() {
			if (!hasAdvices()) {
				type = Type.EXTENT;
				return;
				//TODO remove?
				//throw new IllegalStateException("Should we really allow this??");
			}
			
			//Each QA stands for one OR term, so we have to check all of them.
			hasCollectionConstraint = false;
			for (int a = 0; a < advices.size(); a++) {
				//TODO why??
				hasCollectionConstraint |= advices.get(a).hasCollectionConstraint();
			}
			if (hasCollectionConstraint) {
				//TODO why?? -> Simply assign in constructor??
				type = QueryAdvice.Type.COLLECTION;
			} else {
				type = advices.get(0).getType(); 
			}
		}
		public QueryAdvice.Type getType() {
			return type;
		}
		public boolean hasCollectionConstraint() {
			return hasCollectionConstraint;
		}
		public static VariableInstance[] cloneForConcurrentExecution(VariableInstance[] orig) {
			VariableInstance[] ret = new VariableInstance[orig.length];
			for (int i = 0; i < ret.length; i++) {
				ret[i] = orig[i].cloneForConcurrentExecution();
			}
			return ret;
		}
		private VariableInstance cloneForConcurrentExecution() {
			//We can use the same list here, because the list will never be changed now.
			VariableInstance vi = new VariableInstance(var, advices);
			//vi.advices.addAll(advices);
			//vi.value = null;
			//Iterator<?> iter; 
			vi.hasCollectionConstraint = hasCollectionConstraint;
			vi.type = type;
			return vi;
		}
	}

	
	private Object runQueryV4(Iterable<?> ext, long rangeMin, long rangeMax, String resultSettings, 
			Class<?> resultClass, boolean ignoreCache, Object[] params) {
		rangeMin = queryTree.getRangeMin(rangeMin, params);
		rangeMax = queryTree.getRangeMax(rangeMax, params);
		
		//assign parameters
		//TODO still required???
		//TODO still required???
		//TODO still required???
		//TODO still required???
	//	assignParametersToQueryTree();
		
		
		//This is only for indices, not for given extents
		//TODO avoid optimizing query if we have no parameters or if it cannot improve things!
		//TODO avoid optimizing query if we have no parameters or if it cannot improve things!
		//TODO avoid optimizing query if we have no parameters or if it cannot improve things!
		//TODO avoid optimizing query if we have no parameters or if it cannot improve things!
		//TODO avoid optimizing query if we have no parameters or if it cannot improve things!
		//TODO avoid optimizing query if we have no parameters or if it cannot improve things!
		//TODO (re-)optimize only if:
		//     (- not optimized since last compilation)
		//     - has parameters
		//     - parameters are on an indexed fields and index is used
		//     - there is more than one index (otherwise we only update the QueryAdvice instance!!! 
		//This returns one QUeryAdvice for each variable, +1 for the main_variable 
		VariableInstance[] vars;
		if (optimizerResult == null || queryTree.requiresRerunForChangedParams()) {
			vars = queryTree.executeOptimizerV4(candClsDef, variableDeclarations, params);
			optimizerResult = vars;
		} else {
			//Multi-threading: every query execution requires it's own set of variables.
			//Whoever creates the query can of course use the 'optimizer' result.
			//Every execution that comes later has to work on a copy (unless we would somehow 
			//  synchronize everything and result the variables.
			//So the simplest solution is that we simply close the variables.
			vars = VariableInstance.cloneForConcurrentExecution(optimizerResult);
			//TODO is cloning always necessary? Even if there are no parameters?
		}

		//TODO can also return a list with (yet) unknown size. In that case size() should return
		//Integer.MAX_VALUE (JDO 2.2 14.6.1)
		Iterator<Object> ret = applyQueryOnExtentV4(queryTree, vars, ext, ignoreCache, params);

		//Result merging is (usually) required when we have more than one QueryAdvice,
		//see discussion in QueryOptimizer.
		boolean mergeResults = vars[0].advices.size() > 1;
		if (mergeResults) {
			LOGGER.info("Merging query results");
			ObjectIdentitySet<Object> ret2 = new ObjectIdentitySet<Object>();
			while (ret.hasNext()) {
				ret2.add(ret.next());
			}
			if (ret2.size() > 1000) {
				LOGGER.warn("Merged > 1000 query results");
			}
			return postProcessV4(ret2.iterator(), rangeMin, rangeMax, resultSettings, resultClass);
		}
		
		return postProcessV4(ret, rangeMin, rangeMax, resultSettings, resultClass);
	}

	private void assignParametersToQueryTree() {
		if (parameters.isEmpty()) {
			return;
		}
		if (queryTree.getRootFn() != null) {
			//query-function V4
			return;
		}

		//TODO
		//TODO
		//TODO
		//TODO We should install an subscription service here. Every term/function that
		//TODO uses a ParameterDeclaration should subscribe to the Parameter and get updated when
		//TODO the parameter changes. Parameters without subscriptions should cause errors/warnings.
		//TODO
		//TODO
		//TODO
		QueryTreeIterator iter = queryTree.termIterator();
		while (iter.hasNext()) {
			QueryTerm term = iter.next();
			if (!term.isParametrized()) {
				continue;
			}
			String pName = term.getParamName();
			//TODO cache Fields in QueryTerm to avoid String comparison?
			boolean isAssigned = false;
			for (ParameterDeclaration param: parameters) {
				if (pName.equals(param.getName())) {
					//TODO assigning a parameter instead of the value means that the query will
					//adapt new values even if it is not recompiled.
					term.setParameter(param);
					//check
					if (param.getType() == null) {
						throw DBLogger.newUser(
								"Parameter has not been declared: " + param.getName());
					}
					isAssigned = true;
					break;
				}
			}
			//TODO Exception?
			if (!isAssigned) {
				System.out.println("WARNING: Query parameter is not assigned: \"" + pName + "\"");
			}
		}
	}

	private Object postProcessV3(Collection<Object> c, long rangeMin, long rangeMax, 
			String resultSettings, Class<?> resultClass) {
		if (resultSettings != null) {
			QueryResultProcessor rp = 
					new QueryResultProcessor(resultSettings, candCls, candClsDef, resultClass);
			if (rp.isProjection()) {
				c = rp.processResultProjection(c.iterator(), unique);
			} else {
				//must be an aggregate
				Object o = rp.processResultAggregation(c.iterator());
				return o;
			}
		}
		if (unique) {
			//unique
			Iterator<Object> iter = c.iterator();
			if (iter.hasNext()) {
				Object ret = iter.next();
				if (iter.hasNext()) {
					throw DBLogger.newUser("Too many results found in unique query.");
				}
				return ret;
			} else {
				//no result found
				return null;
			}
		}
		if (ordering != null && !ordering.isEmpty()) {
			if (!(c instanceof List)) {
				c = new ArrayList<>(c);
			}
			Collections.sort((List<Object>) c, new QueryComparator<Object>(ordering));
		}
		
		//To void remove() calls
		return new SynchronizedROCollection<>(c, pm, rangeMin, rangeMax);
	}
	
	private Object postProcessV4(Iterator<Object> iter, long rangeMin, long rangeMax, 
			String resultSettings, Class<?> resultClass) {
		ArrayList<Object> list = null;
		if (resultSettings != null) {
			QueryResultProcessor rp = 
					new QueryResultProcessor(resultSettings, candCls, candClsDef, resultClass);
			if (rp.isProjection()) {
				list = rp.processResultProjection(iter, unique);
				//TODO project to iterator!!!
				iter = list.iterator();
			} else {
				//must be an aggregate
				return rp.processResultAggregation(iter);
			}
		}
		if (unique) {
			//unique
			if (iter.hasNext()) {
				Object ret = iter.next();
				if (iter.hasNext()) {
					throw DBLogger.newUser("Too many results found in unique query.");
				}
				return ret;
			} else {
				//no result found
				return null;
			}
		}
		if (ordering != null && !ordering.isEmpty()) {
			if (list == null) {
				list = new ArrayList<>();
				while (iter.hasNext()) {
					list.add(iter.next());
				}
			}
			//TODO log message for sorting in memory!!
			Collections.sort(list, new QueryComparator<Object>(ordering));
		}

		//TODO Fix this! SynchedCollection should use Iterator!!!
		//TODO Fix this! SynchedCollection should use Iterator!!!
		//TODO Fix this! SynchedCollection should use Iterator!!!
		//TODO Fix this! SynchedCollection should use Iterator!!!
		//TODO Fix this! SynchedCollection should use Iterator!!!
		if (list == null) {
			list = new ArrayList<>();
			while (iter.hasNext()) {
				list.add(iter.next());
			}
		}

		
		//To void remove() calls
		return new SynchronizedROCollection<>(list, pm, rangeMin, rangeMax);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void applyQueryOnExtentV3(Iterable<?> ext, List<Object> ret, QueryAdvice qa, 
			boolean ignoreCache, Object[] params) {
		QueryTree queryTree = qa.getQuery();
		Iterator<?> ext2;
		if (!ignoreCache) {
			ClientSessionCache cache = pm.internalGetCache();
			cache.persistReachableObjects();
		}
		if (qa.getIndex() != null) {
			ext2 = pm.getPrimaryNode().readObjectFromIndex(qa.getIndex(),
					qa.getMin(), qa.getMax(), !ignoreCache);
			if (!ignoreCache) {
				ClientSessionCache cache = pm.internalGetCache();
				ArrayList<ZooPC> dirtyObjs = cache.getDirtyObjects();
				if (!dirtyObjs.isEmpty()) {
					QueryMergingIterator<ZooPC> qmi = new QueryMergingIterator();
					qmi.add((Iterator<ZooPC>) ext2);
					qmi.add(cache.iterator(candClsDef, subClasses, ObjectState.PERSISTENT_NEW));
					ext2 = qmi;
				}
			}
		} else {
			DBLogger.LOGGER.warn("query.execute() found no index to use");
			if (DBStatistics.isEnabled()) {
				pm.statsInc(STATS.QU_EXECUTED_WITHOUT_INDEX);
				if (!ordering.isEmpty()) {
					pm.statsInc(STATS.QU_EXECUTED_WITH_ORDERING_WITHOUT_INDEX);
				}
			}
			//use extent
			if (ext != null) {
				//use user-defined extent
				ext2 = ext.iterator();
			} else {
				//create type extent
				ext2 = new ClassExtent(
						candClsDef, candCls, subClasses, pm, ignoreCache).iterator();
			}
		}
		
		if (ext != null && !subClasses) {
			// normal iteration
			while (ext2.hasNext()) {
				Object o = ext2.next();
				//TODO this can never be true
				//TODO this can never be true
				//TODO this can never be true
				if (subClasses) {
					if (!candCls.isAssignableFrom(o.getClass())) {
						continue;
					}
				} else {
					if (candCls != o.getClass()) {
						continue;
					}
				}
				boolean isMatch = queryTree.evaluate(o, params);
				if (isMatch) {
					ret.add(o);
				}
			}
		} else {
			// normal iteration (ignoring the possibly existing compatible extent to allow indices)
			while (ext2.hasNext()) {
				Object o = ext2.next();
				boolean isMatch = queryTree.evaluate(o, params);
				if (isMatch) {
					ret.add(o);
				}
			}
		}

		if (!ext2.hasNext() && ext2 instanceof CloseableIterator) {
			((CloseableIterator)ext2).close();
		}
	}

	
	private Iterator<Object> applyQueryOnExtentV4(QueryTree queryTree, VariableInstance[] vars,
			Iterable<?> ext, boolean ignoreCache, Object[] params) {
		if (!ignoreCache) {
			ClientSessionCache cache = pm.internalGetCache();
			cache.persistReachableObjects();
		}

		// normal iteration (ignoring the possibly existing compatible extent to allow indices)
		return new QueryIteratorV4(queryTree.getRootFn(), vars, params, ext, 
				ignoreCache, !ordering.isEmpty(), pm);
	}

}

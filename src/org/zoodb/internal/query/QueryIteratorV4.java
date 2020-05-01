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
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.jdo.ObjectState;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.Session;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.client.session.ClientSessionCache;
import org.zoodb.internal.query.QueryExecutor.VariableInstance;
import org.zoodb.internal.util.CloseableIterator;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.tools.DBStatistics;
import org.zoodb.tools.DBStatistics.STATS;

/**
 * Iterator over a query result.
 * The query tree itself does not (should not) have state, so the state
 * has to be managed in a separate query iterator.
 * 
 * @author Tilmann Zäschke
 */
public class QueryIteratorV4 implements Iterator<Object> {
	
	private final QueryFunction root;
	private final VariableInstance[] vars;
	private final Object[] executionParams;
	private final boolean ignoreCache;
	private final boolean isOrderingRequired;
	private final Session session;
	private boolean finished = false;
	private Object nextVal = null;
	//Id of the Iterator that is used for the outer loop.
	//This is usually the index/extent of the main candidate class,
	//except the query is something like 
	//... WHERE e.set.contains(this) && e.name == 'Fred' VARIABLE MyType e
	private int mainIteratorId = -1;
	
	QueryIteratorV4(QueryFunction root, VariableInstance[] vars, 
			Object[] params, Iterable<?> customIterable,
			boolean ignoreCache, boolean isOrderingRequired, Session session) {
		this.root = root;
		this.vars = vars;
		this.executionParams = params;
		this.ignoreCache = ignoreCache;
		this.isOrderingRequired = isOrderingRequired;
		this.session = session;
		if (vars.length == 1) {
			mainIteratorId = 0;
		} else {
			//Ensure that our main iterator is either an index or an extent, but
			//not derived from a collection field.
			for (int i = 0; i < vars.length; i++) {
				if (vars[i].getType() != QueryAdvice.Type.COLLECTION 
						&& vars[i].getType() != QueryAdvice.Type.IDENTITY) {
					mainIteratorId = i;
					break;
				}
			}
			if (mainIteratorId == -1) {
				//We could not find a Variable without collection constraint
				//TODO we could be more flexible and create/handle 
				//hasCollectionConstraint separately for each OR term.
				throw DBLogger.newUser("Query has no unconstrained variable. Note that"
						+ " ZooDB considers variables as fully unconstrained if it is"
						+ " unconstrained in at least on branch.");
			}
		}
		if (customIterable == null) {
			createIterator(vars[mainIteratorId]);
		} else {
			//custom iterator: Iterator over externally provided collection
			vars[mainIteratorId].setIterator( customIterable.iterator() );
		}
		reset();
	}
	
	public void reset() {
		finished = false;
		nextVal = null;
		//TODO
		//1) Create all required iterators for variables (indexes/extents)
		
		for (int i = 0; i < vars.length; i++) {
			VariableInstance var = vars[i];
			//TODO set-null for constrained query, reset() for unconstrained query
			if (vars[i].getType() == QueryAdvice.Type.COLLECTION) {
				var.setIterator( null );
			}
		}
		
	}
	
	private boolean getNextInner(int varId, Object nextCandidate) {
		VariableInstance var = vars[varId]; 
		//TODO use reset()!!!!
		createIterator(var);
		Iterator<?> iter = var.iter();
		//iter.reset();
		
		//iter==null indicates an unconstrained variable.
		//In this case, the value is set by the query term.
		//Alternative: we get the query term here and calculate it to get the
		//iterator, but would that always work? 
		while (iter == null || iter.hasNext()) {
			var.value = iter == null ? null : iter.next();
			if (varId + 1 < vars.length) {
				if (getNextInner(nextVariableId(varId), nextCandidate)) {
					return true;
				}
			} else {
				Object result = root.evaluateWithIterator(nextCandidate, vars, executionParams);
				if (result instanceof Boolean && ((Boolean)result) == true) {
					nextVal = nextCandidate;
					return true;
				}
			}
		}
		return false;
	}
	
	private int nextVariableId(int varId) {
		return varId + 1 != mainIteratorId ? varId + 1 : varId + 2;
	}
	
	/**
	 * Find the next result or 'null' if there are no more results
	 */
	public void getNext() {
		// 1) Increment root-iterator. If finished-> return null
		// 2) Reset variables (and variable iterators?
		// 3) Evaluate
		// 4) If match: return
		// 5) If !match: goto 1) 
		
		while (vars[0].iter().hasNext()) {
			Object nextCandidate = vars[0].iter().next();
			if (1 < vars.length) {
				if (getNextInner(nextVariableId(0), nextCandidate)) {
					return;
				}
			} else {
				Object result = root.evaluateWithIterator(nextCandidate, vars, executionParams);
				if (result instanceof Boolean && ((Boolean)result) == true) {
					nextVal = nextCandidate;
					return;
				}
			}
		}
		finished = true;
		nextVal = null;
		
		
		//Variables (general discussion, move somewhere else?):
		//1) Building: Identify a term that uses 'VAR == constant' (where constant may be a parameter)
		//             Or at least x < VAR < y.
		//             TODO This doesn't quite work, for example car.ownerid == VAR && VAR == owner.id?
		//2) Building: Create subquery -> goes over extent (yes, possible!) or over index
		//3) Execution: Execute as subquery -> gives a result collection
		//4) Execution: Execute main query for every value of variable until we find a match! 
		//              -> This can be optimized:
		//                 - first execute the part of the query that uses the variable?
		//                 - Or: Remember results of branches that do not use the query..
		//              -> Optimization can be done later! First make it work!!
			
		// car/owner example:
		// 1) identify subqueries in variable-domains
		// 2) determine index for each sub-query
		// 3) First execute subquery with 'best' index.
		//    Problem, this may change with parameters...
		//    -> Ignore for now, using an index at all is a good enough approximation...
		//       possibly use extent-size as measure?
		// 4) Actually: Execute ALL subqueries! Then simply JOIN everything...
		//    -> What about car.owner.name = X?
		//       a) No index (except OID): car-extent->get_each_owner->check name
		//          -> O(n_car)
		//       b) Index on owner.name? No use...  -> O( n_car * log (n_owner) )
		//          Does not (hardly) matter if we first look up people by name an search cars for owner.id
		//          or if we traverse all cars and directly look up the names.
		//       c) Ideal A: Path-Index car.owner.name = 'x'
		//          -> O(n_(car.owner.name=='x') )   ---   O( log (n_car) )
		//       d) Ideal B: Index on owner.name && index on car.owner
		//          -> O(n_(owner.name=='x') * n_(car.owner) ) --- O( log(n_owner) * log(n_car) )
		//       e) Index on car.owner?
        //          Traverse cars -> lookup owners -> check names: O(n_cars * log(n_owner) )
		//          Traverse owners -> check names -> lookup cars: O(n_owners + n_matching_owners*log(n_cars) )
		//          Index is on useful (second scenario) if n_owners<n_cars,
		//          otherwise first scenario is better.
		
		// -> what about team.emps.contains(varX) && varX.name == 'boss'
		//
	}
	
	private void createIterator(VariableInstance var) {
		Iterator<?> ext2;
		ZooClassDef candClsDef = var.var.getTypeDef();
		Class<?> candCls = var.var.getType();
		//For inner variables, always allow subclasses (?)
		boolean subClasses = true;
		if (var.hasCollectionConstraint()) {
			ext2 = null;
		} else if (var.hasAdvices()) {
			QueryMergingIterator<ZooPC> qmi = null;
			for (int ia = 0; ia < var.getAdvices().size(); ia++) {
				QueryAdvice qa = var.getAdvices().get(ia);
				if (!qa.hasCollectionConstraint() && !qa.hasIdentityConstraint()) {
					qmi = qmi != null ? qmi : new QueryMergingIterator<>(); 
					qmi.add( session.getPrimaryNode().readObjectFromIndex(qa.getIndex(),
							qa.getMin(), qa.getMax(), !ignoreCache) );
				}
			}
			if (qmi == null) {
				//No iterator required
				return;
			}
			if (!ignoreCache) {
				ClientSessionCache cache = session.internalGetCache();
				ArrayList<ZooPC> dirtyObjs = cache.getDirtyObjects();
				if (!dirtyObjs.isEmpty()) {
					qmi.add(cache.iterator(candClsDef, subClasses, ObjectState.PERSISTENT_NEW));
				}
			}
			ext2 = qmi;
		} else {
			DBLogger.LOGGER.warn("query.execute() found no index to use");
			if (DBStatistics.isEnabled()) {
				session.statsInc(STATS.QU_EXECUTED_WITHOUT_INDEX);
				if (isOrderingRequired) {
					session.statsInc(STATS.QU_EXECUTED_WITH_ORDERING_WITHOUT_INDEX);
				}
			}
			//use extent
			//create type extent
			ext2 = new ClassExtent<>(candClsDef, candCls, subClasses, session, ignoreCache).iterator();
		}

		var.setIterator(ext2);
	}
	
	static class Value {
		CloseableIterator<?> iter;
		//null indicates 'not set'
		Object variableValue = null;
		
		void reset() {
			variableValue = null;
		}
	}

	@Override
	public boolean hasNext() {
		if (finished) {
			return false;
		}
		if (nextVal != null) {
			return true;
		}
		getNext();
		if (nextVal == null) {
			finished = true;
			return false;
		}
		return true;
	}

	@Override
	public Object next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		Object ret = nextVal;
		nextVal = null;
		return ret;
	}
}

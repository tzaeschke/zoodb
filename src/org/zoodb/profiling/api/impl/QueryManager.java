package org.zoodb.profiling.api.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.zoodb.jdo.QueryImpl;
import org.zoodb.jdo.internal.query.QueryParameter;

public class QueryManager {
	
	private Map<Integer,QueryProfile> queryProfiles;
	private int nextQueryNr = 1;
	
	public QueryManager() {
		queryProfiles = new HashMap<Integer,QueryProfile>();
	}
	
	public void insertProfile(int queryId, QueryImpl query, int pageCountBegin) {
		/*
		 * Get Id of query
		 * if query already exists, update based on id
		 * 
		 * if query does not yet exist, create new query profile
		 */
		QueryProfile qp = queryProfiles.get(queryId);
		
		if (qp == null) {
			// 1st execution of this query
			qp = new QueryProfile(queryId);
			qp.setNr(nextQueryNr);
			nextQueryNr++;
			
			qp.setCandidateClass(query.getCandidateClass());
			qp.setResultClass(query.getResultClass());
			qp.setExcludeSubclasses(query.isSubClasses());
			qp.setFilterClause(query.getFilter());
			qp.setGroupClause(null);
			qp.setIgnoreCache(query.getIgnoreCache());
			qp.setImportClause(null);
			qp.setOrderClause(null);
			qp.setResult(query.getResultClause());
			qp.setUnique(query.isUnique());
			qp.setVariables(null);
			
			List<QueryParameter> params = query.getParameters();
			for (QueryParameter p : params) {
				qp.addParameter(p);
			}
			
			
			//execution count, per trx
			// do not update execution count, query could be cancelled before finished
			//qp.updateExecutionCount(ProfilingManager.getInstance().getCurrentTrxId());
			
			queryProfiles.put(queryId, qp);
		} else {
			// query has been executed before, update its specs
		}
		
		qp.setCurrentTrx(ProfilingManager.getCurrentTrx().getId());
		long start = System.currentTimeMillis();
		qp.setCurrentStartTime(start);
		qp.updatePageCount(pageCountBegin);
	}
	
	
	public void updateProfile(int queryId, QueryImpl query, int pageCountEnd) {
		long end = System.currentTimeMillis();
		QueryProfile qp = queryProfiles.get(queryId);
		
		// update execution count
		qp.updateExecutionCount();
		
		// update execution time
		qp.updateExecutionTime(end);
		
		// update pageCount
		qp.updatePageCount(pageCountEnd);
	}
	
	public Collection<QueryProfile> getQueryProfiles() {
		return queryProfiles.values();
	}
	
	public QueryProfile getProfileForQuery(QueryImpl query) {
		Integer id = getId(query);
		return queryProfiles.get(id);
	}
	

	public static int getId(QueryImpl query) {
		int id = 0;
		
		Class<?> candidateClass = query.getCandidateClass();
		Class<?> resultClass	= query.getResultClass();
		String filter			= query.getFilter();
		
		if (candidateClass != null) {
			id += candidateClass.hashCode();
		}
		if (resultClass != null) {
			id += resultClass.hashCode();
		}
		if (filter != null) {
			id += filter.hashCode();
		}
		/*
		 * TODO: include subclasses,unique
		 */
		
		return id;
	}

}

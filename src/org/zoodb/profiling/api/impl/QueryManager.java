package org.zoodb.profiling.api.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.zoodb.jdo.QueryImpl;

public class QueryManager {
	
	private Map<Integer,QueryProfile> queryProfiles;
	
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
			qp.setParameters(null);
			
			//execution count, per trx
			// do not update execution count, query could be cancelled before finished
			//qp.updateExecutionCount(ProfilingManager.getInstance().getCurrentTrxId());
			
			queryProfiles.put(queryId, qp);
		} else {
			// query has been executed before, update its specs
		}
		
		qp.setCurrentTrx(ProfilingManager.getInstance().getCurrentTrxId());
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
			

}

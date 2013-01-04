package org.zoodb.profiling.api.impl;

import java.util.LinkedHashMap;
import java.util.Map;

public class QueryProfile {
	
	private int id;
	
	private String currentTrx;
	private long currentStartTime;
	
	/*
	 * JDO Query Options
	 */
	private Class<?> candidateClass;
	private Class<?> resultClass;
	
	private String filterClause;
	private String parameters;
	private String variables;
	private String importClause;
	private String orderClause;
	private String groupClause;
	private String result;
	
	private boolean unique;
	private boolean excludeSubclasses;
	private boolean ignoreCache;
	
	/*
	 * How to save range information? 
	 * - save all (can be a lot) 
	 * vs.
	 * - save (min(lower),max(upper))
	 * vs.
	 * 	?
	 */
	
	
	/**
	 * Save executionTimes per transaction
	 */
	private Map<String,Long> executionTimes;
	
	/**
	 * Save executionCounts per transaction 
	 */
	private Map<String,Integer> executionCounts;
	
	/**
	 * Save cancellations per transaction 
	 */
	private Map<String,Integer> cancelCounts;
	
	public QueryProfile(int id) {
		this.id = id;
		
		// by using LinkedHashMap we get an implicit ordering of the transactions (global and per PM) for free
		executionTimes = new LinkedHashMap<String,Long>();
		executionCounts = new LinkedHashMap<String,Integer>();
		cancelCounts = new LinkedHashMap<String,Integer>();
	}

	public Class<?> getCandidateClass() {
		return candidateClass;
	}

	public void setCandidateClass(Class<?> candidateClass) {
		this.candidateClass = candidateClass;
	}

	public Class<?> getResultClass() {
		return resultClass;
	}

	public void setResultClass(Class<?> resultClass) {
		this.resultClass = resultClass;
	}

	public String getFilterClause() {
		return filterClause;
	}

	public void setFilterClause(String filterClause) {
		this.filterClause = filterClause;
	}

	public String getParameters() {
		return parameters;
	}

	public void setParameters(String parameters) {
		this.parameters = parameters;
	}

	public String getVariables() {
		return variables;
	}

	public void setVariables(String variables) {
		this.variables = variables;
	}

	public String getImportClause() {
		return importClause;
	}

	public void setImportClause(String importClause) {
		this.importClause = importClause;
	}

	public String getOrderClause() {
		return orderClause;
	}

	public void setOrderClause(String orderClause) {
		this.orderClause = orderClause;
	}

	public String getGroupClause() {
		return groupClause;
	}

	public void setGroupClause(String groupClause) {
		this.groupClause = groupClause;
	}

	public String getResult() {
		return result;
	}

	public void setResult(String result) {
		this.result = result;
	}

	public boolean isUnique() {
		return unique;
	}

	public void setUnique(boolean unique) {
		this.unique = unique;
	}

	public boolean isExcludeSubclasses() {
		return excludeSubclasses;
	}

	public void setExcludeSubclasses(boolean excludeSubclasses) {
		this.excludeSubclasses = excludeSubclasses;
	}

	public boolean isIgnoreCache() {
		return ignoreCache;
	}

	public void setIgnoreCache(boolean ignoreCache) {
		this.ignoreCache = ignoreCache;
	}
	
	public String getCurrentTrx() {
		return currentTrx;
	}

	public void setCurrentTrx(String currentTrx) {
		this.currentTrx = currentTrx;
	}

	public long getCurrentStartTime() {
		return currentStartTime;
	}

	public void setCurrentStartTime(long currentStartTime) {
		this.currentStartTime = currentStartTime;
	}

	public void updateExecutionCount() {
		Integer i = executionCounts.get(currentTrx);
		if (i==null) {
			i = new Integer(0);
		}
		i++;
		executionCounts.put(currentTrx, i);
		
	}
	
	public void updateExecutionTime(long end) {
		Long execTimesForThisTrx = executionTimes.get(currentTrx);
		if (execTimesForThisTrx==null) {
			execTimesForThisTrx = new Long(0);
		}
		execTimesForThisTrx += (end - currentStartTime);
		executionTimes.put(currentTrx, execTimesForThisTrx);
	}

}

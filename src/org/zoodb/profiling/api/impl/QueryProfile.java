package org.zoodb.profiling.api.impl;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.zoodb.internal.query.QueryParameter;

public class QueryProfile {
	
	private int id;
	
	private int nr;
	
	private String currentTrx;
	private long currentStartTime;
	
	/*
	 * JDO Query Options
	 */
	private Class<?> candidateClass;
	private Class<?> resultClass;
	
	private String filterClause;
	private List<String> parameters;
	private String variables;
	private String importClause;
	private String orderClause;
	private String groupClause;
	private String result;
	
	private boolean unique;
	private boolean excludeSubclasses;
	private boolean ignoreCache;
	
	private int cancelCount;
	
	/**
	 * Name of the index used 
	 */
	private String index;
	

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
	
	/**
	 * IO (pageReads) per transaction
	 */
	private Map<String,Long> pageCounts;
	
	public QueryProfile(int id) {
		this.id = id;
		
		// by using LinkedHashMap we get an implicit ordering of the transactions (global and per PM) for free
		executionTimes = new LinkedHashMap<String,Long>();
		executionCounts = new LinkedHashMap<String,Integer>();
		cancelCounts = new LinkedHashMap<String,Integer>();
		pageCounts = new LinkedHashMap<String,Long>();
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

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(List<String> parameters) {
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
	
	public void updatePageCount(long i) {
		Long pageCount = pageCounts.get(currentTrx);
		if (pageCount == null) {
			pageCount = i;
		} else {
			pageCount = i-pageCount;
		}
		pageCounts.put(currentTrx, pageCount);
	}
	
	public void addParameter(QueryParameter qp) {
		if (parameters == null) {
			parameters = new LinkedList<String>();
		} 
		parameters.add(qp.getName().toString());
	}
	
	public int getCancelCount() {
		return cancelCount;
	}
	
	public void incCancelCount() {
		cancelCount++;
	}

	public Map<String, Long> getExecutionTimes() {
		return executionTimes;
	}
	public Map<String, Integer> getExecutionCounts() {
		return executionCounts;
	}
	public Map<String, Long> getPageCounts() {
		return pageCounts;
	}
	public String getIndex() {
		return index;
	}
	public void setIndex(String index) {
		this.index = index;
	}
	public int getNr() {
		return nr;
	}
	public void setNr(int nr) {
		this.nr = nr;
	}
	
	
}
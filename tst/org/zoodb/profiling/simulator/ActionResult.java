package org.zoodb.profiling.simulator;

public class ActionResult {
	
	private long executionTime;
	private Class<?> actionClass;
	private long pageCount;
	
	public long getExecutionTime() {
		return executionTime;
	}
	public void setExecutionTime(long executionTime) {
		this.executionTime = executionTime;
	}
	public Class<?> getActionClass() {
		return actionClass;
	}
	public void setActionClass(Class<?> actionClass) {
		this.actionClass = actionClass;
	}
	public long getPageCount() {
		return pageCount;
	}
	public void setPageCount(long pageCount) {
		this.pageCount = pageCount;
	}
	
	

}

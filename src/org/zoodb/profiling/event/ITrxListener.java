package org.zoodb.profiling.event;

public interface ITrxListener {
	
	/**
	 * 
	 */
	public void onBegin();
	
	/**
	 * 
	 */
	public void afterCommit();
	
	/**
	 * 
	 */
	public void beforeRollback();

}

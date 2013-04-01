package org.zoodb.profiling.api;


public interface IProfilingManager {
	
	public void save();
	
	public IPathManager getPathManager();
	
	public IFieldManager getFieldManager();
	
	public ITrxManager getTrxManager();
	
	/**
	 * Setup profiling
	 */
	public void init(String tag);
	
	/**
	 * Finalize profiling data
	 */
	public void finish();
	
}

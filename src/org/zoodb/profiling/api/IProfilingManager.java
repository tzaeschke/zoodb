package org.zoodb.profiling.api;

public interface IProfilingManager {
	
	public void save();
	
	public IPathManager getPathManager();
	
	public IFieldManager getFieldManager();
	
}

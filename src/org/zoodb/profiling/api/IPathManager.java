package org.zoodb.profiling.api;

import java.util.List;


public interface IPathManager {
	
	/**
	 * @param a
	 * @param predecessor
	 */
	public void addActivationPathNode(Activation a, Object predecessor);
	
	
	/**
	 * @return
	 */
	public List<IPath> getPaths(); 
	
	
	/**
	 * 
	 */
	public void prettyPrintPaths();
	
	/**
	 * Aggregates all object-paths to class-paths and counts their frequency.
	 */
	public void aggregateObjectPaths();
	
	/**
	 * 
	 */
	public void prettyPrintClassPaths();
	
}

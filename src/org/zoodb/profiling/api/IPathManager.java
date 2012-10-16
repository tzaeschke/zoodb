package org.zoodb.profiling.api;

import java.util.List;


public interface IPathManager {
	
	/**
	 * @param a
	 * @param predecessor
	 */
	public void addActivationPathNode(Activation a, Object predecessor);
	
	/**
	 * Prints all object based path trees including their full activation node.
	 */
	public void prettyPrintPaths();
	
	/**
	 * Aggregates all object-paths to class-paths and counts their frequency.
	 */
	public void aggregateObjectPaths();
	
	/**
	 * @param classLevelTrees if true prints all classLevelTrees in a class-based fashion. Otherwise prints all objectLevelTrees in a class-based fashion
	 */
	public void prettyPrintClassPaths(boolean classLevelTrees);
	
}

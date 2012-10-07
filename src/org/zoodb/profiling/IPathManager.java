package org.zoodb.profiling;

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
}

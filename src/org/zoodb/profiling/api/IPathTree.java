package org.zoodb.profiling.api;

/**
 * @author tobiasg
 *
 */
public interface IPathTree {
	
	/**
	 * @return the root of the path tree
	 */
	public IPathTreeNode getRoot();
	
	/**
	 * @return true if the path is a list (only single-child nodes)
	 */
	public boolean isList();
	
	/**
	 * Returns the path node which is of class 'clazzName' and has objectIdentifier 'oid'.
	 * Returns null if such a nodes does not exist. 
	 * @param clazzName
	 * @param oid
	 * @return
	 */
	public IPathTreeNode getNode(String clazzName, String oid);
	
	public void prettyPrintWithTrigger();
}

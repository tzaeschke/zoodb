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
	
	/**
	 * Returns the path node which is of class 'clazzName'
	 * Returns null if such a nodes does not exist.
	 * @param clazzName
	 * @return
	 */
	public IPathTreeNode getNode(String clazzName);
	
	/**
	 * Returns the path node which is of class 'clazzName' and was triggered by 'triggerName'
	 * Returns null if such a nodes does not exist.
	 * @param clazzName
	 * @return
	 */
	public IPathTreeNode getNode(IPathTreeNode currentNode);
	
	
	public void prettyPrintClassPaths();
	
	
	/**
	 * Pretty prints all object path trees including their full activation node. 
	 */
	public void prettyPrint();
}

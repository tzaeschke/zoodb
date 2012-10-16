package org.zoodb.profiling.api;

import java.util.List;

import org.zoodb.profiling.api.impl.PathTreeNode;

public interface IPathTreeNode {

	/**
	 * @return The Activation item associated with this node.
	 */
	public Activation getItem();

	public String getClazz();

	/**
	 * @return The list of children associated with this node.
	 */
	public List<IPathTreeNode> getChildren();

	/**
	 * Pretty prints this node and all its children with associated indentation.
	 * @param indent
	 */
	public void prettyPrint(int indent);

	public List<Class> getActivatorClasses(List<Class> classList);

	/**
	 * @return true if this tree with root at this node is a list.
	 * Motivation:
	 * List-shaped paths could be optimized in 2 ways:
	 *  - direct reference to tail objects
	 *  - direct access by initial query
	 */
	public boolean isList();

	/**
	 * Increases the access frequency of this node by 1. 
	 */
	public void incAccessFrequency();
	
	/**
	 * Returns the access frequency of this node
	 * @return
	 */
	public int getAccessFrequency();
	

	/**
	 * @param newChild Adds a children to this nodes children collection
	 */
	public void addChildren(PathTreeNode newChild);
	
	/**
	 * Returns the path node which is of class 'clazzName' and has objectIdentifier 'oid'.
	 * Returns null if such a nodes does not exist. 
	 * @param clazzName
	 * @param oid
	 * @return
	 */
	public IPathTreeNode getNode(String clazzName, String oid);
	
	
	/**
	 * Returns the path node which is of class 'clazzName'.
	 * Returns null if such a nodes does not exist. 
	 * @param clazzName
	 * @param oid
	 * @return
	 */
	public IPathTreeNode getNode(String clazzName);
	
	public IPathTreeNode getNode(IPathTreeNode currentNode);

	public void prettyPrintClassPaths(int indent);
	
	/**
	 * @return True if the object was activated at least once
	 * False otherwise
	 */
	public boolean isActivatedObject();
	
	/**
	 * Mark this node as an activated object 
	 */
	public void setActivatedObject();
	
	public String getTriggerName();

}

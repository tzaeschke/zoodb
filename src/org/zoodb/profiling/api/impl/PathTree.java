package org.zoodb.profiling.api.impl;

import java.util.LinkedList;
import java.util.List;

import org.zoodb.profiling.api.IPathTree;
import org.zoodb.profiling.api.IPathTreeNode;


public class PathTree implements IPathTree{
	
	private PathTreeNode root;
	
	public PathTree(PathTreeNode root) {
		this.root = root;
	}
	
	@Override
	public PathTreeNode getRoot() {
		return root;
	}
	
	@Override
	public boolean isList() {
		return root.isList();
	}

	protected IPathTreeNode getPathNode(Object predecessor) {
		return root.getPathNode(predecessor);
	}
	
	protected IPathTreeNode getPathNode(String clazz, String ref, String oid) {
		return root.getPathNode(clazz,ref,oid);
	}
	
	protected IPathTreeNode getPathNodeClass(IPathTreeNode currentNode) {
		return root.getPathNodeClass(currentNode);
	}

	protected void prettyPrint() {
		root.prettyPrint(0);
		
	}
	
	protected void prettyPrintWithClasses() {
		root.prettyPrintWithClasses(0);
		
	}
	

	
	protected List<Class> getActivatorClasses() {
		List<Class> activatorClasses = new LinkedList<Class>();
		return root.getActivatorClasses(activatorClasses);
	}

	@Override
	public IPathTreeNode getNode(String clazzName, String oid) {
		return root.getNode(clazzName,oid);
	}

	@Override
	public void prettyPrintWithTrigger() {
		root.prettyPrintWithTrigger(0);
		
	}
	
	
	
}

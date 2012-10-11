package org.zoodb.profiling.api.impl;

import java.util.LinkedList;
import java.util.List;


public class PathTree {
	
	private PathTreeNode root;
	
	public PathTree(PathTreeNode root) {
		this.root = root;
	}
	
	public PathTreeNode getRoot() {
		return root;
	}

	public PathTreeNode getPathNode(Object predecessor) {
		return root.getPathNode(predecessor);
	}

	public void prettyPrint() {
		root.prettyPrint();
		
	}
	
	public boolean isList() {
		return root.isList();
	}
	
	public List<Class> getActivatorClasses() {
		List<Class> activatorClasses = new LinkedList<Class>();
		return root.getActivatorClasses(activatorClasses);
	}
	
	
	
}

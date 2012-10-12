package org.zoodb.profiling.api.impl;

import java.util.LinkedList;
import java.util.List;


public class PathTree {
	
	private PathTreeNode root;
	
	public PathTree(PathTreeNode root) {
		this.root = root;
	}
	
	protected PathTreeNode getRoot() {
		return root;
	}

	protected PathTreeNode getPathNode(Object predecessor) {
		return root.getPathNode(predecessor);
	}
	
	protected PathTreeNode getPathNode(String clazz, String ref, String oid) {
		return root.getPathNode(clazz,ref,oid);
	}

	protected void prettyPrint() {
		root.prettyPrint(0);
		
	}
	
	protected boolean isList() {
		return root.isList();
	}
	
	protected List<Class> getActivatorClasses() {
		List<Class> activatorClasses = new LinkedList<Class>();
		return root.getActivatorClasses(activatorClasses);
	}
	
	
	
}

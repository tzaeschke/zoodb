package org.zoodb.profiling.api;

import java.util.LinkedList;
import java.util.List;

public class PathTreeNode {
	
	private Activation item;
	private List<PathTreeNode> children;

	public PathTreeNode(Activation item) {
		this.item = item;
		children = new LinkedList<PathTreeNode>();
	}
	
	public void addChildren(PathTreeNode newChildren) {
		children.add(newChildren);
	}
	
	public boolean containsItem() {
		return false;
	}

	public PathTreeNode getPathNode(Object predecessor) {
		//check if self is the predecessor
		//if not go through all children
		if (item.getActivator() == predecessor) {
			return this;
		} else {
			if (children.size() > 0) {
				for (PathTreeNode ptn : children) {
					return ptn.getPathNode(predecessor);
				}
			} else {
				return null;
			}
		}
		return null;
	}
	
	public Activation getItem() {
		return item;
	}

	public void prettyPrint() {
		System.out.println(item.prettyString());
		for (PathTreeNode ptn : children) {
			ptn.prettyPrint();
		}
		
	}

}

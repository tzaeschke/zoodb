package org.zoodb.profiling.api.impl;

import java.util.LinkedList;
import java.util.List;

import org.zoodb.profiling.api.IPathTree;
import org.zoodb.profiling.api.IPathTreeNode;
import org.zoodb.profiling.api.ITreeTraverser;

/**
 * @author tobiasg
 * This class implements an iterative preorder traversal of a tree.
 */
public class TreeTraverser implements ITreeTraverser {
	
	private List<PathTreeNode> stack;
	
	private PathTree tree;
	
	public TreeTraverser(PathTree tree) {
		this.tree = tree;
		stack = new LinkedList<PathTreeNode>();
		stack.add(tree.getRoot());
	}
	
	
	/**
	 * @return next PathTreeNode in traversal order
	 * Remove element from the stack, insert its children and return the element
	 */
	public IPathTreeNode next() {
		if (stack.size() > 0) {
			PathTreeNode currentItem = stack.remove(0);
		
			for (PathTreeNode childNode: currentItem.getChildren()) {
				stack.add(childNode);
			}
			return currentItem;
		} else {
			return null;
		}
	}


	@Override
	public void resetAndInit(IPathTree newTree) {
		// TODO Auto-generated method stub
		
	}
}

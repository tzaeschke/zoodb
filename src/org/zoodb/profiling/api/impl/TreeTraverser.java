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
	
	private List<IPathTreeNode> stack;
	
	public TreeTraverser(PathTree tree) {
		stack = new LinkedList<IPathTreeNode>();
		stack.add(tree.getRoot());
	}
	
	
	/**
	 * @return next PathTreeNode in traversal order
	 * Remove element from the stack, insert its children and return the element
	 */
	public IPathTreeNode next() {
		if (stack.size() > 0) {
			IPathTreeNode currentItem = stack.remove(0);
		
			for (IPathTreeNode childNode: currentItem.getChildren()) {
				stack.add(childNode);
			}
			return currentItem;
		} else {
			return null;
		}
	}


	@Override
	public void resetAndInit(IPathTree newTree) {
		stack = new LinkedList<IPathTreeNode>();
		stack.add(newTree.getRoot());
		
	}
}

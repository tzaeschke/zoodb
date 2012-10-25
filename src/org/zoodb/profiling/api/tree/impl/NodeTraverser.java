package org.zoodb.profiling.api.tree.impl;

import java.util.LinkedList;
import java.util.List;

/**
 * @author tobiasg
 *
 * @param <T>
 */
public class NodeTraverser<T extends AbstractNode> {
	
	private List<T> stack;
	
	public NodeTraverser(T rootNode) {
		stack = new LinkedList<T>();
		stack.add(rootNode);
	}

	@SuppressWarnings("unchecked")
	public T next() {
		if (stack.size() > 0) {
			T currentItem = stack.remove(0);
		
			for (AbstractNode childNode: currentItem.getChildren()) {
				stack.add((T) childNode);
			}
			return currentItem;
		} else {
			return null;
		}
	}

	
	public void resetAndInit(T newRootNode) {
		stack = new LinkedList<T>();
		stack.add(newRootNode);
	}
	

}

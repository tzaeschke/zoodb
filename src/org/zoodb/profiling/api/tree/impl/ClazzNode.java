package org.zoodb.profiling.api.tree.impl;

import java.util.LinkedList;
import java.util.List;

import org.zoodb.profiling.api.IPathTreeNode;

public class ClazzNode extends AbstractNode {
	
	private List<ObjectNode> objectNodes;
	
	public ClazzNode() {
		objectNodes = new LinkedList<ObjectNode>();
	}



	@Override
	public boolean hasChild(AbstractNode node) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String toString() {
		return this.getClazzName();
	}
	
	public void addObjectNode(ObjectNode on) {
		objectNodes.add(on);
	}

	/**
	 * Returns the clazzNode which contains objectNodes similar to currentNode in the tree with root 'this' or null if such a node does not exist. 
	 * TODO: Similar means:
	 * sameClass
	 * sameTrigger
	 * sameTargetClass
	 * @param currentNode
	 * @return
	 */
	public ClazzNode getNode(ObjectNode currentNode) {
		if (objectNodes.size() > 0) {
			ObjectNode referenceNode = objectNodes.get(0);
			
			boolean sameClass = referenceNode.getClazzName().equals(currentNode.getClazzName());
			boolean sameTrigger = referenceNode.getTriggerName().equals(currentNode.getTriggerName());
			boolean sameTargetClass = referenceNode.getActivation().getActivator().getClass().getName().equals(currentNode.getActivation().getActivator().getClass().getName());
			if ( sameClass && sameTrigger && sameTargetClass ) {
				return this;
			} else { 
				if (children.size() > 0) {
					for (AbstractNode child : children) {
						ClazzNode childResult = ( (ClazzNode) child).getNode(currentNode);
						
						if (childResult != null) {
							return childResult;
						}
					}
				} else {
					return null;
				}
			}
			return null;
		} else {
			return null;
		}
		 
		
		
	}
	
	
	/**
	 * Returns the clazzNode in the subtree with root=this which has the parentNode in his objectNode-set
	 * Returns null if such a node does not exist in this subtree.
	 * @param abstractNode
	 * Intended usage: initial transformation of an object tree to a class tree
	 * @return 
	 */
	public ClazzNode getOwnerOfParent(AbstractNode abstractNode) {
		if (objectNodes.contains(abstractNode)) {
			return this;
		} else {
			if (children.size() > 0 ){
				for (AbstractNode child : children) {
					ClazzNode result = ((ClazzNode) child).getOwnerOfParent(abstractNode);
					if (result !=null) {
						return result;
					}
				}
			} else {
				return null;
			}
			
		}
		return null;
	}

}

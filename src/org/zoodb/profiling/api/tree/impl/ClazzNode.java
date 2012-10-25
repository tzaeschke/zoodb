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
	public boolean isList() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean hasChild(AbstractNode node) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public void addObjectNode(ObjectNode on) {
		objectNodes.add(on);
	}

	/**
	 * Returns the clazzNode which contains objectNodes similar to currentNode in the tree with root 'this' or null if such a node does not exist. 
	 * TODO: Similar means:
	 * @param currentNode
	 * @return
	 */
	public ClazzNode getNode(ObjectNode currentNode) {
		if (objectNodes.size() > 0) {
			ObjectNode referenceNode = objectNodes.get(0);
			
			if ( referenceNode.getClazzName().equals(currentNode.getClazzName()) && referenceNode.getTriggerName().equals(currentNode.getTriggerName()) && referenceNode.getActivation().getActivator().getClass().getName().equals(currentNode.getActivation().getActivator().getClass().getName())   ) {
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

}

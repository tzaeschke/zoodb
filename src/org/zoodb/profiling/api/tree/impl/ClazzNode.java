package org.zoodb.profiling.api.tree.impl;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.zoodb.profiling.api.Activation;
import org.zoodb.profiling.api.IPathTreeNode;

public class ClazzNode extends AbstractNode {
	
	private List<ObjectNode> objectNodes;
	private Map<Long,Long> predecessorIndex;
	
	private long size;
	
	public ClazzNode() {
		objectNodes = new LinkedList<ObjectNode>();
		predecessorIndex = new HashMap<Long,Long>();
	}



	@Override
	public boolean hasChild(AbstractNode node) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append(this.getClazzName());
		
		for (ObjectNode on : objectNodes) {
			sb.append(" ");
			sb.append(on.getObjectId());
		}
		return sb.toString();
	}
	
	public void addObjectNode(ObjectNode on) {
		objectNodes.add(on);
	}
	
	public void addNode(Activation a) {
		size += a.getActivatorOid();
		predecessorIndex.put(a.getActivatorOid(), a.getPredecessorOid());
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
			//boolean sameTargetClass = referenceNode.getActivation().getActivator().getClass().getName().equals(currentNode.getActivation().getActivator().getClass().getName());
			boolean sameTargetClass = referenceNode.getActivation().getActivatorClass().getName().equals(currentNode.getActivation().getActivatorClass().getName());
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



	public ClazzNode getNodeWithClass(String clazzName) {
		if (this.getClazzName().equals(clazzName)) {
			return this;
		} else {
			for (AbstractNode child : children) {
				ClazzNode childResult = ((ClazzNode) child).getNodeWithClass(clazzName);
				
				if (childResult != null) {
					return childResult;
				}
			}
		}
		return null;
	}
	
	/**
	 * Returns the associated objectNodes for this clazzNode
	 * @return
	 */
	public List<ObjectNode> getObjectNodes() {
		return this.objectNodes;
	}
	
	
	/**
	 * Returns the first ObjectNode in the subtree with root=this which has the same class and same trigger
	 * @param clazzName
	 * @param trigger
	 * @return
	 */
	public ClazzNode getNode(String clazzName, String trigger) {
		if (this.clazzName.equals(clazzName) && this.triggerName.equals(trigger)) {
			return this;
		} else {
			if (children.size() > 0) {
				for ( AbstractNode child : children) {
					ClazzNode childResult = ((ClazzNode) child).getNode(clazzName,trigger);
					
					if (childResult != null) {
						return childResult;
					}
				}
			} else {
				return null;
			}
		}
		return null;
	}

}

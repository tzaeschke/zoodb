package org.zoodb.profiling.api.tree.impl;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zoodb.profiling.api.IPathTreeNode;

/**
 * @author tobiasg
 *
 */
public abstract class AbstractNode {
	
	protected List<AbstractNode> children;
	
	private AbstractNode parentNode;
	
	protected boolean activated = false;
	
	private Logger logger = LogManager.getLogger("allLogger");
	
	protected String triggerName;
	protected String clazzName;
	
	
	public AbstractNode() {
		children = new LinkedList<AbstractNode>();
	}
	
	public void addChild(AbstractNode newChild) {
		newChild.setParentNode(this);
		children.add(newChild);
	}
	
	public List<AbstractNode> getChildren() {
		return children;
	}

	public boolean isActivated() {
		return activated;
	}

	public void setActivated(boolean activated) {
		this.activated = activated;
	}
	
	public String getTriggerName() {
		return triggerName;
	}

	public void setTriggerName(String triggerName) {
		this.triggerName = triggerName;
	}
	
	public String getClazzName() {
		return clazzName;
	}

	public void setClazzName(String clazzName) {
		this.clazzName = clazzName;
	}

	public AbstractNode getParentNode() {
		return parentNode;
	}

	public void setParentNode(AbstractNode parentNode) {
		this.parentNode = parentNode;
	}
	
	public boolean hasChild(AbstractNode node) {
		boolean result=false;
		for (AbstractNode child : children) {
			if(child == node && node.getParentNode() == this) {
				result=true;
				break;
			}
		}
		return result;
	}

	public boolean isList() {
		int childrenCount = children.size();
		if (childrenCount > 1) {
			// if this node has more than 1 activated children it is not a list
			int childIdx = 0;
			int activatedChildrenCount =0;
			for (int i=0;i<childrenCount;i++) {
				if (children.get(i).isActivated()) {
					activatedChildrenCount++;
					childIdx = i;
				}
			}
			if (activatedChildrenCount == 1) {
				return children.get(childIdx).isActivated();
			} else {
				return false;
			}
			
		} else if (childrenCount == 1 && children.get(0).isActivated()) {
			return children.get(0).isList();
		} else {
			return true;
		}
	}

	/**
	 * 
	 */
	public abstract String toString();
	
	
	public void prettyPrint(int indent) {
		String space = "";
		for (int i=0;i<indent;i++) {
			space +="\t";
		}
		indent++;
		logger.info(space + "-->" + this.toString());
		logger.info("");
		for (AbstractNode child : children) {
			child.prettyPrint(indent);
		}
		
	}
	
	/**
	 * More readable version of prettyPrint. Prints only class and trigger info.
	 * @param indent
	 */
	public void prettyPrintClassAndTrigger(int indent) {
		String space = "";
		for (int i=0;i<indent;i++) {
			space +="\t";
		}
		indent++;
		logger.info(space + "-->" + triggerName + ": " + clazzName);
		logger.info("");
		for (AbstractNode child : children) {
			child.prettyPrintClassAndTrigger(indent);
		}
	}

	
	
	
	
	
	

}

package org.zoodb.profiling.api.impl;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zoodb.profiling.api.Activation;
import org.zoodb.profiling.api.IPathTreeNode;

public class PathTreeNode implements IPathTreeNode {
	
	private Activation data;
	private List<IPathTreeNode> children;
	
	private String clazz;
	private String ref;
	private String oid;
	private String triggerName;
	
	private boolean activatedObject = false;
	
	private int accessFrequency=1; 
	
	private Logger logger = LogManager.getLogger("allLogger");

	public PathTreeNode(Activation item) {
		this.data = item;
		children = new LinkedList<IPathTreeNode>();
	}
	
	public void addChildren(PathTreeNode newChildren) {
		children.add(newChildren);
	}
	
	public List<IPathTreeNode> getChildren() {
		return children;
	}
	
	@Override
	public Activation getItem() {
		return data;
	}
	
	@Override
	public void prettyPrint(int indent) {
		String space = "";
		for (int i=0;i<indent;i++) {
			space +="\t";
		}
		indent++;
		logger.info(space + "-->" + accessFrequency + "# " + data.prettyString());
		logger.info("");
		for (IPathTreeNode ptn : children) {
			ptn.prettyPrint(indent);
		}
		
	}
	
	@Override
	public boolean isList() {
		int childrenCount = children.size();
		if (childrenCount > 1) {
			// if this node has more than 1 activated children it is not a list
			int childIdx = 0;
			int activatedChildrenCount =0;
			for (int i=0;i<childrenCount;i++) {
				if (children.get(i).isActivatedObject()) {
					activatedChildrenCount++;
					childIdx = i;
				}
			}
			if (activatedChildrenCount == 1) {
				return children.get(childIdx).isActivatedObject();
			} else {
				return false;
			}
			
		} else if (childrenCount == 1 && children.get(0).isActivatedObject()) {
			return children.get(0).isList();
		} else {
			return true;
		}
		
	}
	
	/**
	 * @return 
	 * TODO: use only if path is list-shaped
	 */
	public List<Class> getActivatorClasses(List<Class> classList) {
		Class activatorClass = data.getActivator().getClass();
		classList.add(activatorClass);
		return children.size() == 0 ? classList : children.get(0).getActivatorClasses(classList) ;  
	}

	public String getClazz() {
		return clazz;
	}

	public void setClazz(String clazz) {
		this.clazz = clazz;
	}

	public String getRef() {
		return ref;
	}

	public void setRef(String ref) {
		this.ref = ref;
	}

	public String getOid() {
		return oid;
	}

	public void setOid(String oid) {
		this.oid = oid;
	}
	
	public void incAccessFrequency() {
		this.accessFrequency++;
	}

	public String getTriggerName() {
		return triggerName;
	}

	public void setTriggerName(String triggerName) {
		this.triggerName = triggerName;
	}

	@Override
	public IPathTreeNode getNode(String clazz) {
		if ( this.clazz.equals(clazz)  ) {
			return this;
		} else { 
			if (children.size() > 0) {
				for (IPathTreeNode ptn : children) {
					IPathTreeNode childResult = ptn.getNode(clazz);
					
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
	
	@Override
	public IPathTreeNode getNode(IPathTreeNode currentNode) {
		//if ( this.clazz.equals(currentNode.getClazz()) && this.triggerName.equals(currentNode.getTriggerName())   ) {
		if ( this.clazz.equals(currentNode.getClazz()) && this.triggerName.equals(currentNode.getTriggerName()) && this.getItem().getActivator().getClass().getName().equals(currentNode.getItem().getActivator().getClass().getName())   ) {
				return this;
		} else { 
			if (children.size() > 0) {
				for (IPathTreeNode ptn : children) {
					IPathTreeNode childResult = ptn.getNode(currentNode);
					
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

	@Override
	public IPathTreeNode getNode(String clazzName, String oid) {
		if (this.clazz.equals(clazzName) && this.oid.equals(oid)) {
			return this;
		} else {
			if (children.size() > 0) {
				for (IPathTreeNode ptn : children) {
					IPathTreeNode childResult = ptn.getNode(clazzName,oid);
					
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

	@Override
	public void prettyPrintClassPaths(int indent) {
		String space = "";
		for (int i=0;i<indent;i++) {
			space += "\t";
		}
		indent++;
		logger.info(space + "--> (" + triggerName + ") #"+ accessFrequency + " " + clazz + " " + activatedObject);
		logger.info("");
		for (IPathTreeNode ptn : children) {
			ptn.prettyPrintClassPaths(indent);
		}
		
	}

	@Override
	public boolean isActivatedObject() {
		return activatedObject;
	}
	
	@Override
	public void setActivatedObject() {
		this.activatedObject = true;
	}

	@Override
	public int getAccessFrequency() {
		return this.accessFrequency;
	}

	@Override
	public boolean hasChild(IPathTreeNode a) {
		boolean result = false;
		for (IPathTreeNode child : children ) {
			if (child.getItem().equals(a.getItem())) {
				result = true;
				break;
			}
		}
		return result;
	}

	
	
	

}

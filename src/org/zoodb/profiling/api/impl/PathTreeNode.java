package org.zoodb.profiling.api.impl;

import java.util.LinkedList;
import java.util.List;

import org.zoodb.profiling.api.Activation;
import org.zoodb.profiling.api.IPathTreeNode;

public class PathTreeNode implements IPathTreeNode {
	
	private Activation data;
	private List<IPathTreeNode> children;
	
	private String clazz;
	private String ref;
	private String oid;
	private String triggerName;
	
	private boolean realAccess = false;
	
	private int accessFrequency=1; 

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
		for (int i=0;i<indent;i++) {
			System.out.print("\t");
		}
		indent++;
		System.out.print("-->" + accessFrequency + "# " + data.prettyString());
		System.out.println();
		for (IPathTreeNode ptn : children) {
			ptn.prettyPrint(indent);
		}
		
	}
	
	@Override
	public boolean isList() {
		int childrenCount = children.size();
		if (childrenCount > 1) {
			return false;
		} else if (childrenCount == 1) {
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
		for (int i=0;i<indent;i++) {
			System.out.print("\t");
		}
		indent++;
		System.out.print("--> (" + triggerName + ") #"+ accessFrequency + " " + clazz);
		System.out.println();
		for (IPathTreeNode ptn : children) {
			ptn.prettyPrintClassPaths(indent);
		}
		
	}
	
	

}

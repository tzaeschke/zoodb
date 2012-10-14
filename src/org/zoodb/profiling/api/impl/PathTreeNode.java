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
	
	public IPathTreeNode getPathNode(Object predecessor) {
		//check if self is the predecessor
		//if not go through all children
		if (data.getActivator() == predecessor) {
			return this;
		} else {
			if (children.size() > 0) {
				for (IPathTreeNode ptn : children) {
					return ptn.getPathNode(predecessor);
				}
			} else {
				return null;
			}
		}
		return null;
	}
	
	public IPathTreeNode getPathNode(String clazz, String ref, String oid) {
		if (this.clazz.equals(clazz) && this.ref.equals(ref) ) {
			return this;
		} else { 
			if (children.size() > 0) {
				for (IPathTreeNode ptn : children) {
					IPathTreeNode childResult = ptn.getPathNode(clazz,ref,oid);
					
					if (childResult != null) {
						return childResult;
					}
					//return ptn.getPathNode(clazz,ref,oid);
				}
			} else {
				return null;
			}
		}
		return null;
	}
	
	public Activation getItem() {
		return data;
	}

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
	
	public void prettyPrintWithClasses(int indent) {
		for (int i=0;i<indent;i++) {
			System.out.print("\t");
		}
		indent++;
		System.out.print("-->" + accessFrequency + "# " + clazz);
		System.out.println();
		for (IPathTreeNode ptn : children) {
			ptn.prettyPrint(indent);
		}
		
	}

	/**
	 * @return 
	 * Motivation:
	 * List-shaped paths could be optimized in 2 ways:
	 *  - direct reference to tail objects
	 *  - direct access by initial query
	 */
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

	public IPathTreeNode getPathNodeClass(IPathTreeNode currentNode) {
		if ( this.clazz.equals(currentNode.getClazz()) && this.data.getMemberResult().getClass().getName().equals(currentNode.getItem().getMemberResult().getClass().getName()) ) {
			return this;
		} else { 
			if (children.size() > 0) {
				for (IPathTreeNode ptn : children) {
					IPathTreeNode childResult = ptn.getPathNodeClass(currentNode);
					
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

	public void prettyPrintWithTrigger(int indent) {
		for (int i=0;i<indent;i++) {
			System.out.print("\t");
		}
		indent++;
		System.out.print("-->" + triggerName + clazz);
		System.out.println();
		for (IPathTreeNode ptn : children) {
			ptn.prettyPrintWithTrigger(indent);
		}
		
	}
	
	

}

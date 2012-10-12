package org.zoodb.profiling.api.impl;

import java.util.LinkedList;
import java.util.List;

import org.zoodb.profiling.api.Activation;

public class PathTreeNode {
	
	private Activation data;
	private List<PathTreeNode> children;
	
	private String clazz;
	private String ref;
	private String oid;

	public PathTreeNode(Activation item) {
		this.data = item;
		children = new LinkedList<PathTreeNode>();
	}
	
	public void addChildren(PathTreeNode newChildren) {
		children.add(newChildren);
	}
	
	public boolean containsItem() {
		return false;
	}

	public PathTreeNode getPathNode(Object predecessor) {
		//check if self is the predecessor
		//if not go through all children
		if (data.getActivator() == predecessor) {
			return this;
		} else {
			if (children.size() > 0) {
				for (PathTreeNode ptn : children) {
					return ptn.getPathNode(predecessor);
				}
			} else {
				return null;
			}
		}
		return null;
	}
	
	public PathTreeNode getPathNode(String clazz, String ref, String oid) {
		if (this.clazz.equals(clazz) && this.ref.equals(ref) ) {
			return this;
		} else { 
			if (children.size() > 0) {
				for (PathTreeNode ptn : children) {
					return ptn.getPathNode(clazz,ref,oid);
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
		System.out.print("-->" + data.prettyString());
		System.out.println();
		for (PathTreeNode ptn : children) {
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
	
	

}

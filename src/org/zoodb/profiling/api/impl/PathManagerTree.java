package org.zoodb.profiling.api.impl;

import java.util.LinkedList;
import java.util.List;

import org.zoodb.profiling.api.Activation;
import org.zoodb.profiling.api.IPath;
import org.zoodb.profiling.api.IPathManager;

public class PathManagerTree implements IPathManager {
	
	private List<PathTree> pathTrees;
	
	public PathManagerTree() {
		pathTrees = new LinkedList<PathTree>();
	}

	@Override
	public void addActivationPathNode(Activation a, Object predecessor) {
		/**
		 * Find tree where predecessor is in:
		 * Can predecessor be in multiple trees? Yes (if objects are shared), but no information gained when knowing 
		 * from which tree it originated --> use first one
		 */
		PathTreeNode firstPredecessor = findTree(predecessor);
		if (firstPredecessor == null) {
			PathTreeNode secondPredecessor = findTree(a.getActivator());
			
			if (secondPredecessor == null) {
				PathTree pt = new PathTree(new PathTreeNode(a));
				pathTrees.add(pt);
			} else {
				secondPredecessor.addChildren(new PathTreeNode(a));
			}
		} else {
			firstPredecessor.addChildren(new PathTreeNode(a));
		}
	}
	
	private PathTreeNode findTree(Object predecessor) {
		PathTreeNode predecessorNode = null;
		for (PathTree pt : pathTrees) {
			
			predecessorNode= pt.getPathNode(predecessor);
			if (predecessorNode != null) {
				break;
			}
		}
		
		return predecessorNode;
	}
	
	/**
	 * @param a Activation to be checked. Motivation: we do not want access to non-reference fields to trigger an activation (would start a new path tree) 
	 * @return
	 */
	private boolean isFakePathItem(Activation a) {
		boolean isRootOfAnyTree = false;
		for (PathTree pt : pathTrees) {
			Activation rootActivation = pt.getRoot().getItem();
			if (rootActivation.getActivator().hashCode() == a.getActivator().hashCode() && rootActivation.getActivator().jdoZooGetOid() == a.getActivator().jdoZooGetOid()) {
				isRootOfAnyTree = true;
				break;
			}
		}
		return isRootOfAnyTree;
	}
	

	@Override
	public List<IPath> getPaths() {
		// TODO: implement behaviour for non-list shaped paths
		for (PathTree pt: pathTrees) {
			if (pt.isList()) {
				prettyPrintPath(pt.getActivatorClasses());
			}
		}
		
		return null;
	}

	@Override
	public void prettyPrintPaths() {
		for (PathTree pt : pathTrees) {
			System.out.println("Starting new path tree...");
			
			pt.prettyPrint();
		}

	}
	
	private void prettyPrintPath(List<Class> activatorClasses) {
		int indent=0;
		for (Class clazz: activatorClasses) {
			for (int i=0;i<indent;i++) {
				System.out.print("\t");
			}
			indent++;
			System.out.print("-->" + clazz.getName());
			System.out.println();
			
		}
	}

}

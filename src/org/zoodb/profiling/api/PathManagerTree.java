package org.zoodb.profiling.api;

import java.util.LinkedList;
import java.util.List;

public class PathManagerTree implements IPathManager {
	
	private List<PathTree> pathTrees;
	
	public PathManagerTree() {
		pathTrees = new LinkedList<PathTree>();
	}

	@Override
	public void addActivationPathNode(Activation a, Object predecessor) {
		if (predecessor == null) {
			/**
			 * Prevent 'fake paths': paths of length 1 with access to root nodes member fields
			 */
			if ( !isFakePathItem(a) ) {
				PathTree pt = new PathTree(new PathTreeNode(a));
				pathTrees.add(pt);
			} 
			
		} else {
			/**
			 * Find tree where predecessor is in:
			 * Can predecessor be in multiple trees? Yes (if objects are shared), but no information gained when knowing 
			 * from which tree it originated --> use first one
			 */
			PathTreeNode firstPredecessor = findTree(predecessor);
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prettyPrintPaths() {
		for (PathTree pt : pathTrees) {
			System.out.println("Starting new path tree...");
			
			pt.prettyPrint();
		}

	}

}

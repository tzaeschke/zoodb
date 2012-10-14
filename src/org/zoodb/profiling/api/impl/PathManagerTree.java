package org.zoodb.profiling.api.impl;

import java.util.LinkedList;
import java.util.List;

import org.zoodb.profiling.api.Activation;
import org.zoodb.profiling.api.IPath;
import org.zoodb.profiling.api.IPathManager;
import org.zoodb.profiling.api.IPathTreeNode;
import org.zoodb.profiling.api.ITreeTraverser;

public class PathManagerTree implements IPathManager {
	
	private List<PathTree> pathTrees;
	private List<PathTree> classLevelPathTrees;
	
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
		
		String clazz = a.getActivator().getClass().getName();
		String ref = String.valueOf(a.getActivator().hashCode());
		
		PathTreeNode nodeForInsertion = findTree(clazz,ref,null);
		if (nodeForInsertion == null) {
			PathTreeNode rootNode = new PathTreeNode(a);
			rootNode.setClazz(clazz);
			rootNode.setRef(ref);
			
			PathTreeNode rootChildren = new PathTreeNode(a);
			try {
				rootChildren.setClazz(a.getMemberResult().getClass().getName());
				rootChildren.setRef(String.valueOf(a.getMemberResult().hashCode()));

				rootNode.addChildren(rootChildren);
				PathTree pt = new PathTree(rootNode);
				pathTrees.add(pt);
			} catch(Exception e) {
				
			}
		} else {
			/**
			 * On collection access via get(index), memberResult will be null. 
			 * Path will not be broken due to activations triggered when collection is loaded for all collection member.
			 */
			if (a.getMemberResult() != null) {
				PathTreeNode newChild = new PathTreeNode(a);
				newChild.setClazz(a.getMemberResult().getClass().getName());
				newChild.setRef(String.valueOf(a.getMemberResult().hashCode()));
				nodeForInsertion.addChildren(newChild);
			}
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
	
	private PathTreeNode findTree(String clazz, String ref, String oid) {
		PathTreeNode predecessorNode = null;
		for (PathTree pt : pathTrees) {
			
			predecessorNode= pt.getPathNode(clazz,ref,oid);
			if (predecessorNode != null) {
				break;
			}
		}
		return predecessorNode;
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
	
	public void prettyPrintClassPaths() {
		for (PathTree pt : classLevelPathTrees) {
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
	
	/**
	 * Aggregates all object-paths to class-paths and counts their frequency.
	 */
	public void aggregateObjectPaths() {
		classLevelPathTrees = new LinkedList<PathTree>();
		
		int pathTreeCount = pathTrees.size();
		
		classLevelPathTrees.add(pathTrees.get(0));
		
		for (int i=1;i<pathTreeCount;i++) {
			overlayPathTree(pathTrees.get(i));
		}
		
		
	}

	/**
	 * @param pathTree will be integrated in the already existing class-level path trees.
	 * For each node in the pathTree, find it in one of the already existing class-level trees and insert its children nodes (children nodes only!)
	 */
	private void overlayPathTree(PathTree pathTree) {
		ITreeTraverser traverser = new TreeTraverser(pathTree);
		IPathTreeNode currentNode = null;
		
		while ( (currentNode = traverser.next()) != null) {
			//search the node in all class-level trees, take the first one that matches (i.e. currentNode already exists there)
			PathTreeNode matchedNode = null;
			for (PathTree clpt : classLevelPathTrees) {
				matchedNode = clpt.getPathNodeClass(currentNode);
				
				if (matchedNode != null) {
					matchedNode.incAccessFrequency();
			
					break;
				}
			}
		}
		
		

		//pathTree.getRoot()
		
	}
	
	

}

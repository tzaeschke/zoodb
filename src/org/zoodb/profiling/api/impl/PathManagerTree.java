package org.zoodb.profiling.api.impl;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zoodb.profiling.api.Activation;
import org.zoodb.profiling.api.IPath;
import org.zoodb.profiling.api.IPathManager;
import org.zoodb.profiling.api.IPathTreeNode;
import org.zoodb.profiling.api.ITreeTraverser;
import org.zoodb.profiling.suggestion.ListSuggestion;

/**
 * @author tobiasg
 *
 */
public class PathManagerTree implements IPathManager {
	
	private List<PathTree> pathTrees;
	private List<PathTree> classLevelPathTrees;
	private Logger logger = LogManager.getLogger("allLogger");
	
	public PathManagerTree() {
		pathTrees = new LinkedList<PathTree>();
	}
	
	@Override
	public void addActivationPathNode(Activation a, Object predecessor) {
		/**
		 * predecessor == null indicates that object was returned by a query
		 */
		if (predecessor == null) {
			IPathTreeNode fatherNode = findNode(a.getActivator().getClass().getName(),a.getOid());
			
			if (fatherNode == null) {
				PathTreeNode rootNode = new PathTreeNode(a);
				rootNode.setClazz(a.getActivator().getClass().getName());
				rootNode.setOid(a.getOid());
				rootNode.setTriggerName("_query");
				rootNode.setActivatedObject();
				
				PathTreeNode rootChildren = new PathTreeNode(a);
				try {
					rootChildren.setClazz(a.getMemberResult().getClass().getName());
					rootChildren.setOid(a.getTargetOid());
					rootChildren.setTriggerName(a.getMemberName());

					rootNode.addChildren(rootChildren);
					PathTree pt = new PathTree(rootNode);
					pathTrees.add(pt);
				} catch(Exception e) {
					
				}
			} else {
				/**
				 * Traversal of another branch of same node.
				 */
				PathTreeNode rootChildren = new PathTreeNode(a);
				
				try {
					rootChildren.setClazz(a.getMemberResult().getClass().getName());
					rootChildren.setOid(a.getTargetOid());
					rootChildren.setTriggerName(a.getMemberName());
					
					//it is possible that 'fatherNode' already has this child (e.g. if fatherNode is returned by the query (query root-node)
					
					if (fatherNode.hasChild(rootChildren)) {
						logger.info("Same child");
					} else {
						fatherNode.addChildren(rootChildren);
					}
					
					//fatherNode.addChildren(rootChildren);
					
				} catch(Exception e) {
					
				}

			}
			
		} else {
			IPathTreeNode fatherNode = findNode(a.getActivator().getClass().getName(),a.getOid());
			fatherNode.setActivatedObject();
			
			//collection fix
			if (a.getMemberResult() != null) {
				
				
				PathTreeNode newChild = new PathTreeNode(a);
				newChild.setClazz(a.getMemberResult().getClass().getName());
				newChild.setOid(a.getTargetOid());
				newChild.setTriggerName(a.getMemberName());
				
				//it is possible that 'fatherNode' already has this child (e.g. if fatherNode is an object which is shared accross multiple objects)
				//if this is the case, then both their activations are equal
				if (fatherNode.hasChild(newChild)) {
					logger.info("Same child");
				} else {
					fatherNode.addChildren(newChild);
				}
			}
		}
	}
	
	
	/**
	 * @param clazzName
	 * @param oid
	 * @return The first in any tree that matches (clazzName,oid)
	 */
	private IPathTreeNode findNode(String clazzName, String oid) {
		IPathTreeNode fatherNode = null;
		for (PathTree pt : pathTrees) {
			fatherNode= pt.getNode(clazzName,oid);
			if (fatherNode != null) {
				break;
			}
		}
		return fatherNode;
	}
	

	@Override
	public void prettyPrintPaths() {
		for (PathTree pt : pathTrees) {
			logger.info("Starting printing of new object path tree...");
			pt.prettyPrint();
		}

	}
	
	@Override
	public void prettyPrintClassPaths(boolean classLevelTrees) {
		Collection<PathTree> trees = classLevelTrees ? classLevelPathTrees : pathTrees;
		
		for (PathTree pt : trees) {
			logger.info("Starting new class path tree...");
			
			pt.prettyPrintClassPaths();
		}

	}
	
	@Override
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
			IPathTreeNode matchedNode = null;
			for (PathTree clpt : classLevelPathTrees) {
				//matchedNode = clpt.getNode(currentNode.getClazz());
				matchedNode = clpt.getNode(currentNode);
				
				if (matchedNode != null) {
					matchedNode.incAccessFrequency();
			
					break;
				}
			}
		}
	}
	
	@Override
	public void optimizeListPaths() {
		logger.info("Analyzing list paths...");
		ListAnalyzer la = new ListAnalyzer();
		for (PathTree pt : classLevelPathTrees) {
			if (pt.isList()) {
				
				Collection<ListSuggestion> listSuggestions = la.analyzeList(pt);
				
			}
		}
		
	}
	
	

}